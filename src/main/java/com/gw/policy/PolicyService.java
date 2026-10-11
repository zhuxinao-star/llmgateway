package com.gw.policy;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.store.StatsDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 策略模块：运行时状态唯一权威。
 * - 评分（优先级 × 健康 × 速度 × 费用）
 * - EWMA TTFT、熔断状态机、429 冷却
 * - 当日 token 预算、并发限制
 * 依赖规则：stats 只发事件，状态变更全部经由此类。
 */
@Service
public class PolicyService {

    private static final Logger log = LoggerFactory.getLogger(PolicyService.class);

    private final ConfigService configService;
    private final StatsDao dao;
    private final AdaptiveRateLimiter rateLimiter;

    // EntryRuntime key: providerId + "\n" + model —— 熔断与 EWMA 按 提供商×模型 维度
    private final ConcurrentHashMap<String, EntryRuntime> entries = new ConcurrentHashMap<>();
    // ChannelRuntime key: providerId + "\n" + channelId —— Key 级冷却
    private final ConcurrentHashMap<String, ChannelRuntime> channels = new ConcurrentHashMap<>();
    // 用量：provider×model 与 provider 两层；channel 级第三层
    private final ConcurrentHashMap<String, UsageCounter> usage = new ConcurrentHashMap<>();
    /** 大上下文阈值：输入超过此 token 数的请求，其 TTFT 不进速度 EWMA（预填充开销不代表模型速度） */
    private static final long LARGE_CONTEXT_TOKENS = 10_000;
    /** 首字"极慢"判定阈值（ms）：与 streamHeartbeatTimeoutMs 对齐。recentTtftWin 里该值以上的样本
     *  记为"极慢样本"，累积到阈值即踢出分摊抽签席——一次 230s 的尖峰不该被中位数稀释掉。 */
    private static final long VERY_SLOW_TTFT_MS = 30_000;
    /** 极慢样本达到此数量即踢出抽签席（窗口内 2 个 = 该模型在真实流量下反复极慢，不是偶发） */
    private static final int VERY_SLOW_KICK_THRESHOLD = 2;
    // 未进抽签席的原因码：界面按码分类计数（不能让前端去猜 note 文案，文案会改、码不会）
    public static final String BAND_DUP_KEY = "BAND_DUP_KEY";          // 同模型多把 Key 合占一席，本行是重复 Key
    public static final String BAND_SLOW = "BAND_SLOW";                // 实测首字中位慢于阈值
    public static final String BAND_VERY_SLOW = "BAND_VERY_SLOW";      // 窗口内有反复极慢样本
    public static final String BAND_RATE_LIMIT = "BAND_RATE_LIMIT";    // 近窗口持续撞限流（本轮新增）
    public static final String BAND_NO_SEAT = "BAND_NO_SEAT";          // 分数排在席外（席位数上限）
    private final ConcurrentHashMap<String, AtomicInteger> inflightEntry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> inflightProvider = new ConcurrentHashMap<>();
    volatile String currentDay = LocalDate.now().toString();

    /** 隔离客户端：其流量只做"验证/体检"，绝不回流选举——不进 EWMA/中位数/吞吐/预算/限流学习、
     *  失败不进主熔断。probe=定时巡检，selfcheck=一键体检（体检的 5K 压缩样本代表不了 100K 真实请求），
     *  reconcile=模型自动停用器的确认探测（只为判定模型死活，不该污染真实评分）。 */
    private static final java.util.Set<String> ISOLATED_CLIENTS = java.util.Set.of("probe", "selfcheck", "reconcile");
    private static boolean isolatedClient(String client) {
        return client != null && ISOLATED_CLIENTS.contains(client);
    }

    // ---- 余额类临时停用（provider\nmodel → 到期时间戳）----
    // 模型自动停用器确认"余额/额度不足"时置位：路由候选里直接跳过该模型（不写 config.yaml，
    // 重启即清）。到期或被确认探测成功后清除。区别于"模型不存在"的永久停用（后者落盘 enabled=false）。
    private final ConcurrentHashMap<String, Long> tempDisabled = new ConcurrentHashMap<>();
    public boolean isTempDisabled(String providerId, String model) {
        Long until = tempDisabled.get(providerId + "\n" + model);
        if (until == null) return false;
        if (System.currentTimeMillis() >= until) { tempDisabled.remove(providerId + "\n" + model); return false; }
        return true;
    }
    public void setTempDisabled(String providerId, String model, long untilMs) {
        tempDisabled.put(providerId + "\n" + model, untilMs);
    }
    public void clearTempDisabled(String providerId, String model) {
        tempDisabled.remove(providerId + "\n" + model);
    }
    /** 当前处于临时停用（含未到期）的 key 集合，供界面展示 */
    public java.util.Set<String> tempDisabledKeys() {
        long now = System.currentTimeMillis();
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        tempDisabled.forEach((k, until) -> { if (until > now) out.add(k); });
        return out;
    }

    // ---- 早停嫌疑（client×provider×model → 惩罚截止时间戳）----
    // 带 tools 的请求若"经历过压缩 + 回复极短 + 无 tool_calls"，高度疑似静默早停
    //（agent 会误以为任务完成直接收尾）。命中后该 client 对该模型 10 分钟内额外 ×0.5 降权
    //（叠在 toolWeak 因子之上）；同 client 该模型后续真实发起工具调用即解除。
    private static final long EARLY_STOP_TTL_MS = 600_000;
    private static final int EARLY_STOP_MAX_CHARS = 24;
    private final ConcurrentHashMap<String, Long> earlyStopPenalty = new ConcurrentHashMap<>();

    // ---- #33 toolWeak 自动学习（半自动：攒样本 → 出建议 → 人审一键打标，绝不自动改配置）----
    // provider/model → 带 tools 请求样本数（仅 status=OK 完成请求累计，防把"慢/死"误学成"弱工具"）
    private final ConcurrentHashMap<String, AtomicInteger> toolsRequestsSeen = new ConcurrentHashMap<>();
    // provider/model → 历史上是否真实回过 tool_calls（一旦为 true 永不再建议；重启清零可接受，攒样快）
    private final ConcurrentHashMap<String, Boolean> toolCallsEverSeen = new ConcurrentHashMap<>();
    // provider/model → 已标 toolWeak 却又回了 tool_calls 的"矛盾"次数（反证展示，不自动摘标）
    private final ConcurrentHashMap<String, AtomicInteger> toolWeakContradictions = new ConcurrentHashMap<>();

    static String earlyStopKey(String client, String providerId, String model) {
        return client + "\n" + providerId + "\n" + model;
    }

    public PolicyService(ConfigService configService, StatsDao dao, AdaptiveRateLimiter rateLimiter) {
        this.configService = configService;
        this.dao = dao;
        this.rateLimiter = rateLimiter == null ? new AdaptiveRateLimiter(configService) : rateLimiter;
        restoreTodayUsage();
        Thread.ofVirtual().name("gw-day-roller").start(this::dayRollLoop);
    }

    private void dayRollLoop() {
        while (true) {
            try {
                Thread.sleep(60_000);
                String today = LocalDate.now().toString();
                if (!today.equals(currentDay)) {
                    currentDay = today;
                    usage.clear();
                    log.info("日期切换，当日额度计数器已重置: {}", today);
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void restoreTodayUsage() {
        for (StatsDao.UsageRow r : dao.loadUsageOfDay(currentDay)) {
            UsageCounter c = usage.computeIfAbsent(entryKey(r.providerId(), r.model()), k -> new UsageCounter());
            c.requests.addAndGet(r.reqCount());
            c.tokensIn.addAndGet(r.tokensIn());
            c.tokensOut.addAndGet(r.tokensOut());
        }
    }

    static String entryKey(String providerId, String model) { return providerId + "\n" + model; }
    // \u0001 命名空间标记：usage 表同一 Map 里放 entry 与 channel 两类行，channel 行必须以标记开头，
    // 否则 tokensUsedProvider 按 "provider\nxxx" 汇总时会把 channel 行当模型行重复累加（实测 2 倍虚高）
    static String channelKey(String providerId, String channelId) { return providerId + "\n\u0001" + channelId; }

    public static class EntryRuntime {
        public volatile double ewmaTtftMs = -1;    // -1 = 无样本
        public volatile double ewmaTokensPerSec = -1;  // 出 token 速率（EWMA）·-1 = 无样本；吞吐因子
        public volatile int consecutiveFailures = 0;
        public volatile int qualityIssues = 0;      // 软失败次数（截断/空回复/JSON不合法）——单独计，不影响硬熔断
        public volatile long coolDownUntil = 0;     // OPEN 状态冷却截止
        public volatile long backoffMs = 0;         // 当前退避基数
        public volatile boolean halfOpenProbe = false;
        // 半开放行槽：冷却到期后同一时刻只允许一条在途请求去验证恢复（halfOpenBusy=占用中）。
        // halfOpenSince 用于防泄漏：持有者异常退出没回调时，超时后槽自动失效
        public volatile boolean halfOpenBusy = false;
        public volatile long halfOpenSince = 0;
        public volatile String lastError;
        // 最近若干条 TTFT 样本 → 中位数。EWMA 会被偶发超大尖峰钉住（一次 20s 拖高、之后几百 ms
        // 的好样本要几十条才拉回来），拿它当"慢"判据会把偶尔抽风的模型永久逐出抽签席。
        // 中位数抗尖峰：几条快样本就能把尖峰的痕迹挤出中位 → recentTtftMedian。-1=无中位数样本。
        public volatile double recentTtftMedian = -1;
        volatile boolean ttftSeeded;                      // 本进程是否已尝试从日志回填历史最近样本（只查一次库）
        long[] recentTtftWin;                             // 最近 TTFT 环形窗（仅在 synchronized(er) 内读写）
        int recentTtftIdx = 0;
        int recentTtftCount = 0;
    }

    public static class ChannelRuntime {
        public volatile long coolDownUntil = 0;     // 429 等 Key 级冷却
        public volatile long rotationCounter = 0;    // 累计使用次数（诊断展示用）
        public volatile long lastUsedAt = 0;         // 最近一次被选用的时间戳——严格轮转依据
        public volatile long lastRateLimitedAt = 0;  // 最近一次 429 限流的时间戳——解冻后短暂降权依据
    }

    public static class UsageCounter {
        final AtomicLong requests = new AtomicLong();
        final AtomicLong tokensIn = new AtomicLong();
        final AtomicLong tokensOut = new AtomicLong();
    }

    public EntryRuntime entry(String providerId, String model) {
        return entries.computeIfAbsent(entryKey(providerId, model), k -> new EntryRuntime());
    }

    public ChannelRuntime channel(String providerId, String channelId) {
        return channels.computeIfAbsent(channelKey(providerId, channelId), k -> new ChannelRuntime());
    }

    /** 清理某 provider 的全部运行时状态（删除供应商/模型时调用）：熔断、EWMA、冷却、限流学习。
     *  否则删除后用相同 id 重建会继承旧的 coolDownUntil/consecutiveFailures，新供应商上来就被"历史熔断"压住。 */
    public void clearRuntimeFor(String providerId) {
        if (providerId == null) return;
        String prefix = providerId + "\n";
        entries.keySet().removeIf(k -> k.startsWith(prefix));
        channels.keySet().removeIf(k -> k.startsWith(prefix));
        usage.keySet().removeIf(k -> k.startsWith(prefix));
        inflightEntry.keySet().removeIf(k -> k.startsWith(prefix));
        inflightProvider.remove(providerId);
        rateLimiter.reset(providerId);
        String esInfix = "\n" + providerId + "\n";
        earlyStopPenalty.keySet().removeIf(k -> k.contains(esInfix));
        log.info("已清理 provider={} 的运行时状态（熔断/EWMA/冷却/限流学习）", providerId);
    }

    /** 清理单个 (provider, model) 的运行时状态（删除单个模型时调用，避免误伤同 provider 其它模型） */
    public void clearEntryFor(String providerId, String model) {
        if (providerId == null || model == null) return;
        entries.remove(entryKey(providerId, model));
        usage.remove(entryKey(providerId, model));
        inflightEntry.remove(entryKey(providerId, model));
        rateLimiter.removeOutputBudget(providerId, model);
        String esSuffix = "\n" + providerId + "\n" + model;
        earlyStopPenalty.keySet().removeIf(k -> k.endsWith(esSuffix));
        log.info("已清理 {}/{} 的运行时状态", providerId, model);
    }

    // ================= 评分与排序 =================

    /**
     * 候选打分排序。剔除不可用候选（DISABLED/COOLING/预算耗尽/并发满/上下文超限），
     * 返回按分数降序的列表。铁律：**永不返回空**——
     * 全部不可用时强制放行一个最佳兜底（连续失败最少者优先），保证请求总能打出去，
     * 由上游/超时机制给出真实结果，而不是网关直接拒绝。
     * 例外：free-only/paid-only 策略先做费用档位硬过滤，过滤后池子空就是空（用户明令
     * "只走"另一档就不要了，允许失败），由控制器回 503。
     * @param candidates 候选（providerId, model, groupPriority）
     * @param profile critical 时豁免费用因子与预算
     * @param requestCategory 请求语义类别（null=未知/通用；匹配模型擅长的类别加分——语义路由）
     * @param inputTokens 请求输入 token 估算（null=未知不过滤）——超过模型 contextWindow 的候选直接剔除，
     *                    省得在接不住的模型上白烧 attempt（152K 请求打 128K 模型必 400 "Prompt exceeds max length"）
     */
    public List<Scored> rank(List<Candidate> candidates, String strategy, boolean critical,
                             String requestCategory, Integer inputTokens) {
        return rank(candidates, strategy, critical, requestCategory, inputTokens, false, null);
    }

    /** @param hasTools 请求是否带 tools 定义——toolWeak 模型在此类请求上降权（见 score 的 TW 因子） */
    public List<Scored> rank(List<Candidate> candidates, String strategy, boolean critical,
                             String requestCategory, Integer inputTokens, boolean hasTools) {
        return rank(candidates, strategy, critical, requestCategory, inputTokens, hasTools, null);
    }

    /** @param client 接入工具标识（可空）——早停嫌疑惩罚按 client×provider×model 维度生效 */
    public List<Scored> rank(List<Candidate> candidates, String strategy, boolean critical,
                             String requestCategory, Integer inputTokens, boolean hasTools, String client) {
        return rank(candidates, strategy, critical, requestCategory, inputTokens, hasTools, client, null);
    }

    /** @param out 非空时把本次决策快照（候选分数、因子、出局原因、抽签席与主选）写进去，
     *             供"评分与分摊"界面与链路留痕消费。null=热路径默认，不额外分配。 */
    public List<Scored> rank(List<Candidate> candidates, String strategy, boolean critical,
                             String requestCategory, Integer inputTokens, boolean hasTools, String client,
                             RankTrace out) {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        // 预览（评分沙盘）绝不能改变运行态：半开探测槽位一次只能被一个真实请求占用，
        // 若沙盘把槽位抢了，真正的恢复探测就被推迟——预览只读不抢占。
        boolean preview = out != null && out.preview;
        if (out != null) {
            out.ts = System.currentTimeMillis();
            out.strategy = strategy;
            out.critical = critical;
            out.category = requestCategory;
            out.hasTools = hasTools;
            out.inputTokens = inputTokens;
            out.loadShare = d.isLoadShare();
            out.bandSize = Math.max(2, d.getShareBandSize());
            out.slowTtftMs = d.getShareSlowTtftMs();
        }
        // 策略级费用硬过滤：paid-only/free-only 把对侧档位在分类前整批剔除——usable/exhausted/
        // cooling 三个池都进不去，所以"全灭强放行"、"付费保底兜底"等任何路径都不可能把流量交给
        // 用户明令排除的档位（免费全挂就真失败，绝不偷偷走付费；反之亦然）。
        boolean freeOnly = "free-only".equals(strategy);
        boolean paidOnly = "paid-only".equals(strategy);
        List<Scored> usable = new ArrayList<>();
        List<Scored> exhausted = new ArrayList<>();
        List<Scored> cooling = new ArrayList<>();
        // Scored → 它的留痕行：池子顺序会变（排序/兜底/抽签），最后按调用方真正看到的顺序重排 rows
        Map<Scored, RankTrace.Row> rowOf = out == null ? null : new IdentityHashMap<>();
        for (Candidate c : candidates) {
            long now = System.currentTimeMillis();
            ConfigService.Snapshot snap = configService.snapshot();
            if (!snap.channelEnabled(c.providerId, c.channelId())) {
                recordFiltered(out, c, "CHANNEL_DISABLED", "渠道已禁用（管理台关掉），不参与评分");
                continue;
            }
            // 余额类临时停用：确认探测判定该模型"余额不足"，路由直接跳过（到期/成功后自动恢复）
            String tdKey = c.providerId + "\n" + c.model();
            Long tdUntil = tempDisabled.get(tdKey);
            if (tdUntil != null) {
                if (now >= tdUntil) tempDisabled.remove(tdKey);   // 顺带回收到期项
                else {
                    recordFiltered(out, c, "TEMP_DISABLED", "临时停用中（余额/额度不足），约 "
                            + Math.max(1, (tdUntil - now) / 60_000) + " 分钟后自动恢复");
                    continue;
                }
            }
            if (freeOnly || paidOnly) {
                String tier = snap.effectiveCostTier(c.providerId, c.model());
                boolean free = "free".equalsIgnoreCase(tier);
                if (freeOnly ? !free : free) {
                    recordFiltered(out, c, "TIER_BLOCKED", (freeOnly ? "策略=只走免费，但该模型档位是 "
                            : "策略=只走付费，但该模型档位是 ") + (tier == null ? "未标注" : tier));
                    continue;
                }
            }
            // 上下文超限的模型直接出局（配置了 contextWindow 且请求估算超过）。留 1.1 倍余量：
            // 估算是粗略的（字符数/4），贴边上限的让上游自己判
            Integer cw = snap.effectiveContextWindow(c.providerId, c.model());
            if (inputTokens != null && cw != null && inputTokens > cw * 1.1) {
                recordFiltered(out, c, "CONTEXT_TOO_LARGE", "请求约 " + inputTokens + " token 超过该模型上下文 "
                        + cw + "，必被上游拒绝，不参与评分");
                continue;
            }
            ChannelRuntime cr = channel(c.providerId, c.channelId());
            boolean channelCooling = cr.coolDownUntil > now;
            EntryRuntime er = entry(c.providerId, c.model());
            // 熔断冷却：未到期一律冷却态；到期后半开窗口（halfOpenProbe 已置位）内
            // 只放行抢到槽的那一条——其余并发请求继续按冷却处理，等半开结果出来再说。
            // 旧行为是"到期瞬间全量放行"：刚恢复的上游被并发打回熔断，反复震荡。
            boolean entryCooling = er.coolDownUntil > now;
            boolean halfOpenBlocked = false;
            if (!entryCooling && er.coolDownUntil > 0 && er.halfOpenProbe
                    && (preview || !tryClaimHalfOpen(er, now))) {
                entryCooling = true;
                halfOpenBlocked = true;
            }
            boolean budgetExhausted = !critical && isBudgetExhausted(c.providerId, c.model(), c.channelId());
            Integer concLimit = snap.effectiveConcurrencyLimit(c.providerId, c.model(), c.channelId());
            boolean concFull = concLimit != null
                    && inflightEntry.getOrDefault(entryKey(c.providerId, c.model()), new AtomicInteger(0)).get() >= concLimit;

            Map<String, Double> f = out == null ? null : new LinkedHashMap<>();
            double sc = score(c, strategy, critical, d, requestCategory, hasTools, client, f);
            if (channelCooling || entryCooling) {
                Scored s = new Scored(c, sc, true, f);
                cooling.add(s);
                if (out != null) {
                    RankTrace.Row r = out.newRow(c, sc, f);
                    r.state = "COOLING";
                    r.note = coolingNote(cr, er, channelCooling, entryCooling, halfOpenBlocked, preview);
                    rowOf.put(s, r);
                }
                continue;
            }

            Scored s = new Scored(c, sc, budgetExhausted || concFull, f);
            if (s.blocked) exhausted.add(s);
            else usable.add(s);
            if (out != null) {
                RankTrace.Row r = out.newRow(c, sc, f);
                if (s.blocked) {
                    r.state = "BLOCKED";
                    r.note = budgetExhausted ? "当日 token 预算已耗尽" : "并发已满（上限 " + concLimit + "）";
                }
                rowOf.put(s, r);
            }
        }
        usable.sort(Comparator.comparingDouble((Scored s) -> s.score).reversed()
                // 同 (provider,model) 多 Key 候选分数完全相同（评分无 Key 维度）——稳定排序会永远
                // 先选第一把 Key，配了多账号也只是"打死才换"。按最近使用时间轮转：
                // 刚用过的 Key 排后面（lastUsedAt 越小越优先），多把 Key 严格交替分摊——
                // 一号账号被 TPM/RPM 限速时二号账号本来就没在承担流量，轮转让两家都摊。
                // 未用过的 Key（lastUsedAt=0）最优先；并列（如都为 0）时 hash 打破保持确定序。
                .thenComparingLong(s -> channel(s.candidate.providerId(), s.candidate.channelId()).lastUsedAt)
                .thenComparingInt(s -> s.candidate.channelId().hashCode()));
        String fallbackNote = null;
        if (usable.isEmpty()) {
            // 首选：预算/并发耗尽的放行兜底（服务本身大概率仍可用）
            exhausted.sort(Comparator.comparingDouble(s -> s.score));
            if (!exhausted.isEmpty()) {
                usable.add(exhausted.get(0));
                fallbackNote = "无可用候选，放行预算/并发耗尽者兜底";
            } else if (!cooling.isEmpty()) {
                // 全在熔断/冷却/限流：优先放行一个"不在限流压力下"的候选兜底，保证任务不断开
                // （目标：付费模型作低频真兜底——免费都限流了，就用付费保证返回，而不是再撞免费墙）
                Scored guaranteed = pickGuaranteed(candidates, cooling, exhausted, strategy, critical, d, requestCategory);
                if (guaranteed != null) {
                    usable.add(guaranteed);
                    fallbackNote = "免费全挂，放行付费/无压力候选保证任务不断开";
                } else {
                    // 退路：放行连续失败最少的一个强制探测（绝不空手而归）
                    cooling.sort(Comparator.comparingInt(s ->
                            entry(s.candidate.providerId(), s.candidate.model()).consecutiveFailures));
                    usable.add(cooling.get(0));
                    fallbackNote = "全部冷却，强制放行连续失败最少者（铁律：不空手而归）";
                }
            }
        }
        // ---- 分摊抽签（auto/priority 策略、非 critical）：头部 bandSize 个模型（按模型去重不按 Key）
        // 按 √(score/max) 概率抽主选。
        // 背景：赢家通吃——一家 EWMA 快的模型吸走全部流量（样本正反馈让它永远霸榜），
        // 高优先/高编码分的健康模型迟迟拿不到样本"挤不进门"，单家免费上游也被连发容易打爆。
        // 开根号让权重曲线平缓：主选仍是最大概率，但健康邻位分到真实流量——
        // 样本进来后快的分数升、份额自然涨回，慢的最近实测中位数过 shareSlowTtftMs 就不抽签（留在兜底顺序）。
        // critical 不抽（任务要紧 strict 最佳）；latency/cost-first/weighted 不抽（速度/成本/权重语义各有主张）；
        // free-only 照抽——它就是 auto 语义限定在免费池内，分摊压力正是选它的理由；paid-only 不抽
        // （用户要的是"最快最稳最准的那一个"，不是分摊）。
        if (d.isLoadShare() && !critical && usable.size() > 1
                && ("auto".equals(strategy) || "priority".equals(strategy) || freeOnly)) {
            List<Scored> band = new ArrayList<>();
            Set<String> seenModels = new HashSet<>();
            int bandCap = Math.max(2, d.getShareBandSize());
            for (Scored s : usable) {
                if (band.size() >= bandCap) break;
                if (!seenModels.add(s.candidate.providerId() + "\n" + s.candidate.model())) {
                    bandOut(rowOf, s, BAND_DUP_KEY,
                            "同模型的多把 Key 合占一个抽签席（本行是重复 Key，跟首 Key 一起分摊）");
                    continue;
                }
                EntryRuntime er = entry(s.candidate.providerId(), s.candidate.model());
                // 特别慢不抽签——以"最近实测中位数"为准（EWMA 会被一次尖峰钉死）。
                // 无中位数样本（-1）放行：没被抽过恰恰最需要在抽签席上试一枪。
                if (er.recentTtftMedian > d.getShareSlowTtftMs()) {
                    bandOut(rowOf, s, BAND_SLOW, "最近实测首字中位 " + (long) er.recentTtftMedian + "ms 慢于阈值 "
                            + d.getShareSlowTtftMs() + "ms，不占抽签席");
                    continue;
                }
                // 极慢样本硬过滤：中位数会被一次 230s 尖峰稀释掉（环窗里其余都是快样本），
                // 导致"偶尔 4 分钟不出首字"的慢家仍进抽签席、反复被抽中霸占并发位。
                // 窗口内极慢样本（TTFT ≥ streamHeartbeatTimeoutMs）数达到阈值即踢出——
                // 一次偶发尖峰不踢（自愈空间），反复极慢才是真该避让的家。
                int verySlowSamples = verySlowTtftSamples(er);
                if (verySlowSamples >= VERY_SLOW_KICK_THRESHOLD) {
                    bandOut(rowOf, s, BAND_VERY_SLOW, "窗口内有 " + verySlowSamples + " 条极慢样本，踢出抽签席");
                    continue;
                }
                // 限流门槛：这一家近 5 分钟反复撞 429、且学到的额度本来就紧 → 不占抽签席。
                //  背景：抽签按 √(分数) 分摊，头部家哪怕被 L 压到 ×0.35 仍能拿走四成流量；可对
                //  "每分钟只给 2 发"的免费家，给它席位就是按着头让它撞墙，请求还白耗在重试上。
                //  只撤席不剔除：它仍留在兜底顺序里，其它家挂了照样接得住（不空手而归铁律）。
                //  撞墙次数按「家」汇总（一个席位代表这个模型的流量，而 429 是账号额度事实），
                //  额度的紧松也取该家最紧的那把 Key——宽额度家偶发几次 429 只是抖动，不撤席。
                int limits5m = rateLimiter.providerRateLimits(s.candidate.providerId());
                int tightCeiling = rateLimiter.tightestReqCeiling(s.candidate.providerId());
                if (AdaptiveRateLimiter.seatBlockedByRateLimit(limits5m, tightCeiling)) {
                    bandOut(rowOf, s, BAND_RATE_LIMIT, "近 " + (AdaptiveRateLimiter.LIMIT_DENSITY_WINDOW_MS / 60_000)
                            + " 分钟连续撞限流 " + limits5m + " 次（该家学到的额度≈" + tightCeiling
                            + " 次/分），不占抽签席，仍留在兜底顺序");
                    continue;
                }
                band.add(s);
            }
            if (band.size() > 1) {
                double max = Math.max(band.get(0).score, 1e-9);
                double[] w = new double[band.size()];
                double totW = 0;
                for (int i = 0; i < band.size(); i++) {
                    w[i] = Math.sqrt(Math.max(0.0, band.get(i).score) / max);
                    totW += w[i];
                }
                if (out != null) {
                    for (int i = 0; i < band.size(); i++) {
                        RankTrace.Row r = rowOf.get(band.get(i));
                        if (r == null) continue;
                        r.inBand = true;
                        r.share = totW > 0 ? w[i] / totW : 0;   // 应得份额：长期看这个家该接走几成流量
                    }
                }
                double r = java.util.concurrent.ThreadLocalRandom.current().nextDouble(totW);
                Scored chosen = band.get(band.size() - 1);
                for (int i = 0; i < band.size(); i++) {
                    r -= w[i];
                    if (r <= 0) { chosen = band.get(i); break; }
                }
                if (out != null) {
                    RankTrace.Row cr = rowOf.get(chosen);
                    int seat = band.indexOf(chosen) + 1;
                    out.pickReason = (chosen == usable.get(0) ? "分摊抽签命中第 " + seat + " 席（本就到了首位）"
                            : "分摊抽签命中第 " + seat + " 席，置顶取代分数首位")
                            + (fallbackNote == null ? "" : "；" + fallbackNote);
                }
                if (chosen != usable.get(0)) {
                    usable.remove(chosen);   // 按引用移除（Scored 未覆写 equals）
                    usable.add(0, chosen);   // 中签者置顶做主选；原顺序整体保留作失败兜底链
                }
            } else if (out != null) {
                out.pickReason = band.isEmpty() ? "无可抽签席（不足两席），按分数取首位"
                        : "只有 1 个候选进抽签席，无需抽签";
            }
        }
        // weighted 策略：在 top-3 内按权重随机，避免单点
        List<Scored> result = usable;
        if ("weighted".equals(strategy) && usable.size() > 1) {
            List<Scored> top = usable.subList(0, Math.min(3, usable.size()));
            double total = 0;
            for (Scored s : top) total += s.candidate.weight;
            double r = ThreadLocalRandomNorm(total);
            double acc = 0;
            Scored chosen = top.get(top.size() - 1);
            for (Scored s : top) {
                acc += s.candidate.weight;
                if (r <= acc) { chosen = s; break; }
            }
            result = List.of(chosen);
            if (out != null) out.pickReason = "weighted 策略按渠道权重随机";
        }
        if (out != null) {
            if (out.pickReason == null) {
                out.pickReason = fallbackNote != null ? "兜底放行：" + fallbackNote : "按分数取首位（未抽签）";
            }
            finishTrace(out, result, usable, exhausted, cooling, rowOf, bandCapNote(d));
        }
        return result;
    }

    /** 抽签席上限的说明文本（留痕行没进席时的兜底解释） */
    private static String bandCapNote(GatewayConfig.Defaults d) {
        return "未进抽签席（分数排在席外，或该模型已有席位；席上限 " + Math.max(2, d.getShareBandSize()) + "）";
    }

    /** 给留痕行补一句人话解释，已有的不覆盖（冷却/限流的原因更具体） */
    private static void noteIfAbsent(Map<Scored, RankTrace.Row> rowOf, Scored s, String note) {
        if (rowOf == null) return;
        RankTrace.Row r = rowOf.get(s);
        if (r != null && r.note == null) r.note = note;
    }

    /** 标一行"没进抽签席"的原因码 + 人话说明。首写者胜（同 note 规矩）：更具体的门槛先命中就先定性。 */
    private static void bandOut(Map<Scored, RankTrace.Row> rowOf, Scored s, String code, String note) {
        if (rowOf == null) return;
        RankTrace.Row r = rowOf.get(s);
        if (r == null || r.bandOut != null) return;
        r.bandOut = code;
        if (r.note == null) r.note = note;
    }

    private static void recordFiltered(RankTrace out, Candidate c, String reason, String note) {
        if (out == null) return;
        RankTrace.Row r = new RankTrace.Row();
        r.provider = c.providerId();
        r.model = c.model();
        r.channel = c.channelId();
        r.state = "FILTERED";
        r.reason = reason;
        r.note = note;
        out.filtered.add(r);
    }

    /** 冷却原因要说清是哪一层：渠道级冷却 / 模型级熔断 / 半开窗口（真实请求=槽被占，预览=只读不抢槽） */
    private static String coolingNote(ChannelRuntime cr, EntryRuntime er,
                                      boolean channelCooling, boolean entryCooling,
                                      boolean halfOpenBlocked, boolean preview) {
        long now = System.currentTimeMillis();
        if (halfOpenBlocked) {
            return preview ? "熔断已到期、处于半开窗口（只放行一条真实请求验证恢复；沙盘预览只读不抢槽）"
                    : "熔断已到期，但半开探测槽被别的请求占着，本次仍按冷却处理";
        }
        if (channelCooling && entryCooling) {
            return "渠道冷却 " + Math.max(0, (cr.coolDownUntil - now) / 1000) + "s + 模型熔断 "
                    + Math.max(0, (er.coolDownUntil - now) / 1000) + "s（连续失败 " + er.consecutiveFailures + " 次）";
        }
        if (channelCooling) return "渠道被限流/失败冷却，还剩 " + Math.max(0, (cr.coolDownUntil - now) / 1000) + "s";
        return "模型熔断冷却中，还剩 " + Math.max(0, (er.coolDownUntil - now) / 1000) + "s（连续失败 "
                + er.consecutiveFailures + " 次）";
    }

    /**
     * 收尾留痕：rows 按调用方真正看到的顺序重排（usable 头到尾 = 主选 + 失败兜底链），
     * 再把没进结果的 BLOCKED/COOLING 行接在后面——界面要能一眼看出"谁排第几、谁为什么没戏"。
     */
    private void finishTrace(RankTrace out, List<Scored> result, List<Scored> usable,
                             List<Scored> exhausted, List<Scored> cooling,
                             Map<Scored, RankTrace.Row> rowOf, String notInBandNote) {
        List<RankTrace.Row> ordered = new ArrayList<>();
        Set<RankTrace.Row> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (List<Scored> pool : List.of(result, usable, exhausted, cooling)) {
            for (Scored s : pool) {
                RankTrace.Row r = rowOf.get(s);
                if (r == null) {
                    // pickGuaranteed 会新建一个 Scored 包装原候选：按候选坐标找回它那一行
                    r = findRow(out, s.candidate);
                    if (r == null) continue;
                }
                if (seen.add(r)) ordered.add(r);
            }
        }
        for (RankTrace.Row r : out.rows) if (!seen.contains(r)) ordered.add(r);
        out.rows.clear();
        out.rows.addAll(ordered);
        for (RankTrace.Row r : out.rows) {
            if (!r.inBand && r.note == null && "OK".equals(r.state)) {
                r.note = notInBandNote;
                // 只有分摊确实生效时才给"未进席"定性；关着分摊时每条决策都无席，标出来是误导
                if (out.loadShare && r.bandOut == null) r.bandOut = BAND_NO_SEAT;
            }
        }
        if (!result.isEmpty()) {
            RankTrace.Row top = rowOf.get(result.get(0));
            if (top == null) top = out.rows.isEmpty() ? null : findRow(out, result.get(0).candidate);
            if (top != null) {
                top.picked = true;
                out.pickedProvider = top.provider;
                out.pickedModel = top.model;
            }
        }
    }

    private static RankTrace.Row findRow(RankTrace out, Candidate c) {
        for (RankTrace.Row r : out.rows) {
            if (r.provider.equals(c.providerId()) && r.model.equals(c.model()) && r.channel.equals(c.channelId())) {
                return r;
            }
        }
        return null;
    }

    /**
     * 全候选都冷却/限流时的"保任务不断开"兜底选择：
     * 优先挑一个付费模型（non-free）且当前未处于限流压力下的，作为最后一道保证返回；
     * 若没有符合条件的付费，退回冷却中"连续失败最少"的免费探测。
     */
    private Scored pickGuaranteed(List<Candidate> candidates, List<Scored> cooling,
                          List<Scored> exhausted, String strategy, boolean critical,
                          GatewayConfig.Defaults d, String requestCategory) {
        ConfigService.Snapshot snap = configService.snapshot();
        // 1) 优先：付费档 + 无限流压力
        for (Scored s : cooling) {
            String tier = snap.effectiveCostTier(s.candidate.providerId(), s.candidate.model());
            boolean paid = !"free".equalsIgnoreCase(tier) && tier != null;
            ChannelRuntime cr = channel(s.candidate.providerId(), s.candidate.channelId());
            boolean channelCooling = cr.coolDownUntil > System.currentTimeMillis();
            boolean pressured = rateLimiter.ratePressure(s.candidate.providerId(), s.candidate.model(), s.candidate.channelId()) >= 0.9;
            if (paid && !channelCooling && !pressured) {
                log.info("[限流兜底] 免费全挂，放行付费 {} × {} 保证任务不断开", s.candidate.providerId(), s.candidate.model());
                return new Scored(s.candidate, 1.0, false);
            }
        }
        // 2) 其次：任何未处于限流压力的模型（无论付费/免费）
        for (Scored s : cooling) {
            ChannelRuntime cr = channel(s.candidate.providerId(), s.candidate.channelId());
            boolean channelCooling = cr.coolDownUntil > System.currentTimeMillis();
            boolean pressured = rateLimiter.ratePressure(s.candidate.providerId(), s.candidate.model(), s.candidate.channelId()) >= 0.9;
            if (!channelCooling && !pressured) {
                return new Scored(s.candidate, 1.0, false);
            }
        }
        return null;
    }

    private double ThreadLocalRandomNorm(double max) {
        // 结构性还债：热路径此前每候选 new Random()（构造 + 种子开销，weighted 抽签每请求触发）；改共享 ThreadLocalRandom
        return java.util.concurrent.ThreadLocalRandom.current().nextDouble() * max;
    }

    /** Score = P(优先级) × H(健康) × S(首字速度) × T(吞吐) × C(费用) × K(语义类别匹配) × TW(工具弱)
     *  @param f 非空时把每个因子的实际取值写进去，供"评分与分摊"界面解释这一分是怎么来的（热路径传 null，零开销） */
    private double score(Candidate c, String strategy, boolean critical, GatewayConfig.Defaults d,
                         String requestCategory, boolean hasTools, String client, Map<String, Double> f) {
        ConfigService.Snapshot snap = configService.snapshot();
        int priority = snap.effectivePriority(c.providerId, c.model, c.groupPriority);
        EntryRuntime er = entry(c.providerId, c.model);

        // P: 优先级权重——priority 越小越优先。量级刻意收窄（40 档）：
        // P0 vs P1 = 6:1（不再 11:1），让速度/吞吐/语义匹配在同优先级邻域内有实际话语权；
        // C 因子（free→premium 10 倍差）不受影响，付费兜底哲学保持
        double p;
        switch (strategy) {
            case "latency" -> p = 1.0;                       // 速度优先：忽略优先级（此时付费模型可前移）
            case "paid-only" -> p = 1.0;                     // 只走付费=只看快/稳/准，优先级这种"别的"不掺和
            case "cost-first" -> p = 1.0 / (1 + priority * 0.1);
            default -> p = 40.0 / (1 + priority * 5);         // priority/auto：优先级主导但可被 K/S 跨级翻转
        }

        // H: 健康系数——失败次数越多越低；冷却中≈0（兜底放行场景已在上层处理）
        double h = 1.0 / (1 + er.consecutiveFailures * 0.5);

        // Q: 输出质量系数——连续软失败（截断/空回复/JSON不合法）越多越低。
        //  单独于 H（硬失败），因为"能返回但质量差"和"完全失败"是两回事；
        //  免费模型质量参差，这个维度让路由自动避开"经常截断/抽风"的家，只降权不熔断。
        double q = 1.0 / (1 + er.qualityIssues * 0.5);

        // 费用档（S/T 无样本默认与 C 因子共用）
        String tier = snap.effectiveCostTier(c.providerId, c.model);

        // S: 首字速度系数——取"最近实测 TTFT 中位数"（无中位数退回 EWMA）的归一化倒数；
        //  从没见过的模型无样本按费用档给保守默认。
        //  为什么优先中位数：EWMA 一次尖峰（偶发排队/上游抽风）就能把 S 打趴很久，正反馈下
        //  该模型从此抢不到流量、拿不到新样本翻身——"快但偶尔抽风"被误判成"慢"且无法自愈。
        //  中位数只反映最近典型速度，几条快样本即自愈；持续真慢的模型中位数照样慢，不放过。
        //  教训（12:13 付费反超案）：免费模型被大请求刷出慢样本（S≈0.2），付费模型无样本
        //  拿乐观 0.9 → 反超。无样本 = 未知，不能比真实样本更乐观：免费 0.9（鼓励先试）
        //  但付费 0.5（未知速度的付费模型绝不排到有真实样本的免费模型前面）
        ensureRecentTtft(er, c.providerId, c.model);
        double s;
        double ttftEst = er.recentTtftMedian >= 0 ? er.recentTtftMedian : er.ewmaTtftMs;
        if (ttftEst < 0) {
            s = switch (tier) {
                case "free" -> 0.9;
                default -> 0.5;
            };
        } else {
            s = 1000.0 / Math.max(200.0, ttftEst);  // 200ms TTFT 即速度满分 5.0
        }

        // T: 吞吐系数——出 token 速率 EWMA（长回复场景快模型胜出）。
        // 无样本与 S 同哲学：免费 0.9 / 其余 0.5，防"无历史包袱"的付费模型凭默认分反超
        double t;
        if (er.ewmaTokensPerSec < 0) {
            t = switch (tier) {
                case "free" -> 0.9;
                default -> 0.5;
            };
        } else {
            // 20 tok/s 起步给 1.0，200 tok/s 满分 2.5：既要首字快（S）也要出活快（T）
            t = Math.min(2.5, Math.max(0.4, 0.4 + er.ewmaTokensPerSec / 80.0));
        }

        // C: 费用系数——cost-first 下权重↑；free=1.0 递减到 premium=0.1。
        // paid-only 豁免（同 critical）：档位已保证全付费，再按贵贱打分违背"只图快稳准不差钱"的语义
        double cBase = switch (tier) {
            case "free" -> 1.0;
            case "low" -> 0.8;
            case "premium" -> 0.1;
            default -> 0.45;
        };
        double cFactor = (critical || "paid-only".equals(strategy)) ? 1.0
                : ("cost-first".equals(strategy) ? Math.pow(cBase, 2.0) : cBase);

        // K: 语义类别匹配——请求内容判定类别（code/doc/…），模型标签命中 ×1.8（强加分）；
        // 请求未知类别或模型无标签时 1.0（不惩罚）。critical 不放大（它是任务重要性不是内容类型）
        double k = 1.0;
        String modelCategory = snap.effectiveCategory(c.providerId(), c.model());
        if (requestCategory != null && modelCategory != null) {
            k = requestCategory.equals(modelCategory) ? 1.8 : (isGeneralCategory(modelCategory) ? 1.0 : 0.7);
        }

        // CS: 代码能力分 + 性价比自动加权（仅 code 类请求 + 模型打了 codeScore 时生效）。
        //  能力项 = 1 + codeScore/10（0 分→1.0 中性，10 分→2.0）；
        //  性价比 = 能力 × 成本惩罚（越贵惩罚越重：free 1.0 / low 0.9 / standard 0.75 / premium 0.5）
        //  ——效果等价"能力/成本"：能力高且免费最优先；能力高但贵（积分消耗大）自动打折，
        //  不会凭能力分霸榜。惩罚刻意比 C 因子温和，只作"性价比"维度的调节，不重复狠罚。
        //  非 code 请求或无打分 → 1.0（完全不影响现有行为）。
        //  K 与 CS 不重复计能力：K 只管"类别标签命中"，CS 才管"能力高低+性价比"。
        double csFactor = 1.0;
        if ("code".equals(requestCategory) && !critical) {
            Double codeScore = snap.effectiveCodeScore(c.providerId(), c.model());
            if (codeScore != null) {
                double ability = 1.0 + Math.max(0.0, Math.min(10.0, codeScore)) / 10.0;
                // paid-only 不算性价比罚分：用户已声明不看成本，能力分纯净生效（"要准"优先）
                double costPenalty = "paid-only".equals(strategy) ? 1.0 : switch (tier) {
                    case "free" -> 1.0;
                    case "low" -> 0.9;
                    case "standard" -> 0.75;
                    case "premium" -> 0.5;
                    default -> 0.75;
                };
                csFactor = ability * costPenalty;
            }
        }

        // U: 用户自定义权重（管理页手调，乘法生效）——1.0 中性；>1 拔高，<1 压低。
        //  未配置（null）按 1.0，完全不影响现有调度。这是"我自己的偏好"逃生门：
        //  哪怕某家速度/费用不占优，也能凭用户权重把它顶上来。
        double uFactor = 1.0;
        Double uw = snap.effectiveUserWeight(c.providerId(), c.model());
        if (uw != null) uFactor = Math.max(0.1, Math.min(10.0, uw));

        // R: 限流恢复因子——刚刚 429 过的渠道，即使冷却已到期也便宜一块后选。
        //  实测教训：claude-code 0.5s 一发的高频连发，会在渠道 30s 冷却刚到期时立刻重新选中最快模型
        //  再次打爆（EWMA 让它仍排最前），导致"冷却了但等于没冷却"。用一段恢复宽限期把刚从限流
        //  恢复的渠道降权，让连发流量去别的免费家，而非同一家反复撞墙。
        double r = rateLimitRecoverF(snap, c.providerId(), c.channelId());

        // L: 自适应限流因子 = 瞬时窗口压力 与 近窗口撞墙密度 取更狠的一侧。
        //  压力=该渠道当前 60s 用量 / 历史学到的上限（"预测性避让"：不等 429 才冷却，路由就绕开快达上限的家）。
        //  密度=该渠道最近 5 分钟真的撞了几次 429（上限 4 次封顶 → 最多 ×0.35，样本滑出即自动复原）。
        //  为什么必须加密度：免费档学到 2 次/分后，流量一旦被派到别家，窗口用量归零、压力归零、
        //  因子回到 1.0，它又被当作"还有余量"抽中——中签即撞墙，请求白白耗在 429 重试上。
        //  密度按渠道算（一把 Key 撞墙不连坐同家其它 Key）；critical 不放大。无经验上限(从未429)时 1.0(不罚)。
        double limP = 1.0;
        if (!critical) {
            double pressure = rateLimiter.ratePressure(c.providerId(), c.model(), c.channelId());
            double density = rateLimiter.channelLimitDensity(c.providerId(), c.channelId());
            limP = AdaptiveRateLimiter.pressureFactor(pressure, density);
        }

        // TW: 工具能力弱因子——带 tools 定义的请求（Agent 工具发起）打到 toolWeak 模型上，
        //  大概率不回 tool_call 而是"只回一段总结"，agent 误以为任务完成就静默收尾。
        //  软降权 ×0.35（不剔除）：该家仍是唯一可用/其它全挂时照样能接住，正常请求不受影响。
        double tw = 1.0;
        if (hasTools && snap.effectiveToolWeak(c.providerId(), c.model())) tw = 0.35;

        // ES: 早停嫌疑因子——该 client 最近在该模型上出现过"压缩后无工具短回复"（疑似静默早停），
        //  10 分钟 TTL 内带工具请求再叠 ×0.5。与 TW 相乘只是更排后，仍不剔除（该模型可能是唯一可用）。
        double es = 1.0;
        if (hasTools && client != null && !client.isBlank()) {
            String esKey = earlyStopKey(client, c.providerId(), c.model());
            Long exp = earlyStopPenalty.get(esKey);
            if (exp != null) {
                if (exp > System.currentTimeMillis()) es = 0.5;
                else earlyStopPenalty.remove(esKey);
            }
        }
        tw *= es;

        double score = p * h * s * t * cFactor * k * r * limP * csFactor * uFactor * tw;
        score *= q;   // Q 因子乘在最外层：软失败降权但不打断其它因子的平衡
        if (f != null) {
            f.put("P_priority", p);
            f.put("H_health", h);
            f.put("Q_quality", q);
            f.put("S_ttft", s);
            f.put("T_throughput", t);
            f.put("C_costTier", cFactor);
            f.put("K_category", k);
            f.put("CS_codeScore", csFactor);
            f.put("U_userWeight", uFactor);
            f.put("R_rateRecover", r);
            f.put("L_ratePressure", limP);
            f.put("TW_toolWeak", tw / es);   // 拆开存：界面要能看出 0.35 与早停 0.5 各罚了多少
            f.put("ES_earlyStop", es);
            f.put("ttftEstMs", ttftEst);
            // 诊断附值（不连乘）：让界面能直接回答"L 为什么不是 1"——撞墙次数与学到的额度都摊开给看
            f.put("rateLimits5m", (double) rateLimiter.providerRateLimits(c.providerId()));
            f.put("learnedReqPerMin", (double) rateLimiter.tightestReqCeiling(c.providerId()));
            f.put("priority", (double) priority);
            f.put("consecutiveFailures", (double) er.consecutiveFailures);
            f.put("qualityIssues", (double) er.qualityIssues);
        }
        return score;
    }

    /**
     * 限流恢复因子：渠道刚被 429 限流过就降权，且冷却到期后仍有一段"恢复宽限期"继续冷落它。
     *  实测教训：claude-code 0.5s 一发的高频连发，会在渠道 30s 冷却刚到期时立刻重新选中最快模型
     *  再次打爆（EWMA 让它仍排最前），导致"冷却了但等于没冷却"。因此惩罚窗口 = 冷却时长 + 一个
     *  额外的恢复宽限（取 rateLimitCooldownMs 的一半），到期半个冷却时长才能完全恢复正常竞争。
     *  配额耗尽（600s 长冷却）本身已足够长，此惩罚只针对瞬时 TPM/RPM 限流（30s）。
     * @return [0.35, 1.0]：刚限流时最低 0.35，恢复宽限结束时回 1.0。
     */
    private double rateLimitRecoverF(ConfigService.Snapshot snap, String providerId, String channelId) {
        ChannelRuntime cr = channels.get(channelKey(providerId, channelId));
        if (cr == null || cr.lastRateLimitedAt == 0) return 1.0;
        long now = System.currentTimeMillis();
        long elapsed = now - cr.lastRateLimitedAt;       // 距上次 429 的时长
        int cdMs = snap.config.getDefaults().getRateLimitCooldownMs();
        if (cdMs <= 0) return 1.0;
        long penaltyWindow = cdMs + cdMs / 2;            // 冷却 + 半段恢复宽限
        if (elapsed >= penaltyWindow) return 1.0;
        // elapsed ∈ [0, penaltyWindow)：线性从 0.35 回升到 1.0
        double ratio = Math.max(0.0, 1.0 - (double) elapsed / penaltyWindow);
        return 0.35 + 0.65 * ratio;                       // 范围 [0.35, 1.0]
    }

    /** general/通用标签：任何类别都不惩罚（万金油模型） */
    private static boolean isGeneralCategory(String category) {
        return category == null || category.isBlank() || "general".equalsIgnoreCase(category) || "通用".equals(category);
    }

    // ================= 预算 =================

    private boolean isBudgetExhausted(String providerId, String model, String channelId) {
        ConfigService.Snapshot snap = configService.snapshot();
        // 条目级
        Long entryLimit = snap.effectiveDailyTokenLimit(providerId, model);
        if (entryLimit != null && tokensUsed(providerId, model) >= entryLimit) return true;
        // 提供商整包
        GatewayConfig.Provider p = snap.providersById.get(providerId);
        if (p != null && p.getDailyTokenLimit() != null
                && tokensUsedProvider(providerId) >= p.getDailyTokenLimit()) return true;
        // Key 级
        for (GatewayConfig.Channel ch : p == null ? List.<GatewayConfig.Channel>of() : p.getChannels()) {
            if (ch.getId().equals(channelId) && ch.getDailyTokenLimit() != null) {
                UsageCounter uc = usage.get(channelKey(providerId, channelId));
                if (uc != null && uc.tokensIn.get() + uc.tokensOut.get() >= ch.getDailyTokenLimit()) return true;
            }
        }
        return false;
    }

    public long tokensUsed(String providerId, String model) {
        UsageCounter c = usage.get(entryKey(providerId, model));
        return c == null ? 0 : c.tokensIn.get() + c.tokensOut.get();
    }

    public long tokensUsedProvider(String providerId) {
        long sum = 0;
        String prefix = providerId + "\n";
        for (Map.Entry<String, UsageCounter> e : usage.entrySet()) {
            // 只累加 provider×model 行；channel 行（\u0001 标记）跳过——否则同一份 token 记两遍，
            // 提供商级日预算虚高 2 倍，半天就把整家误判成"额度耗尽"踢出路由
            if (e.getKey().startsWith(prefix) && !e.getKey().startsWith(prefix + "\u0001"))
                sum += e.getValue().tokensIn.get() + e.getValue().tokensOut.get();
        }
        return sum;
    }

    public long tokensUsedChannel(String providerId, String channelId) {
        UsageCounter c = usage.get(channelKey(providerId, channelId));
        return c == null ? 0 : c.tokensIn.get() + c.tokensOut.get();
    }

    /** 今日该 Key 扛了多少发（内存日计数器，与 tokensUsedChannel 同一份，重启清零） */
    public long requestsTodayChannel(String providerId, String channelId) {
        UsageCounter c = usage.get(channelKey(providerId, channelId));
        return c == null ? 0 : c.requests.get();
    }

    public long requestsToday(String providerId, String model) {
        UsageCounter c = usage.get(entryKey(providerId, model));
        return c == null ? 0 : c.requests.get();
    }

    // ================= 事件记录（由 stats/forward 调用） =================

    /** 重算最近 TTFT 中位数（调用方持 synchronized(er)）。样本<2 不定中位数：一条尖峰就成了"典型速度"没意义 */
    private static void recomputeRecentMedian(EntryRuntime er) {
        if (er.recentTtftCount < 2) return;
        long[] cp = java.util.Arrays.copyOf(er.recentTtftWin, er.recentTtftCount);
        java.util.Arrays.sort(cp);
        er.recentTtftMedian = cp.length % 2 == 1
                ? cp[cp.length / 2] : (cp[cp.length / 2 - 1] + cp[cp.length / 2]) / 2.0;
    }

    /** 统计窗口内"极慢样本"数（TTFT ≥ VERY_SLOW_TTFT_MS）。调用方持 synchronized(er)。 */
    private static int verySlowTtftSamples(EntryRuntime er) {
        if (er.recentTtftWin == null || er.recentTtftCount == 0) return 0;
        int n = 0;
        for (int i = 0; i < er.recentTtftCount; i++) {
            if (er.recentTtftWin[i] >= VERY_SLOW_TTFT_MS) n++;
        }
        return n;
    }

    /** 进程重启后从请求日志回填"最近实测"TTFT（每条目每进程最多查一次）。
     *  EWMA 一旦带上尖峰样本会把慢判钉死很久——若只用 EWMA 当判据，偶尔抽风的快模型
     *  重启后永远回不了分摊抽签席（没份额→没样本→永远"慢"）。日志里有真相，直接拿来补窗。 */
    private void ensureRecentTtft(EntryRuntime er, String providerId, String model) {
        if (er.ttftSeeded || er.recentTtftCount > 0 || er.recentTtftMedian >= 0) return;
        er.ttftSeeded = true;
        List<Long> hist = dao.recentOkTtfts(providerId, model, 8, LARGE_CONTEXT_TOKENS);   // 新→旧
        if (hist.isEmpty()) return;
        synchronized (er) {
            if (er.recentTtftCount > 0) return;          // 实时样本先到：不画蛇添足
            er.recentTtftWin = new long[Math.max(8, hist.size())];
            for (int i = 0; i < hist.size(); i++) {      // 旧→新填入，后续实时样本从尾部滚动覆盖最旧
                er.recentTtftWin[i] = hist.get(hist.size() - 1 - i);
            }
            er.recentTtftIdx = hist.size() % er.recentTtftWin.length;
            er.recentTtftCount = hist.size();
            recomputeRecentMedian(er);
        }
    }

    /** 请求成功：记录 TTFT 与 usage（client = 接入工具，用于按工具分桶统计）
     *  totalMs = 本请求总时长（流式=首字到流结束；吞吐速率按它算，ttft 只反映首字）
     *
     *  探测隔离（client=probe）：巡检成功只做半开熔断恢复验证，不进速度评分（EWMA/吞吐窗）、
     *  不清真实失败计数、不记预算、不进限流学习——ping 的 10-token 样本
     *  代表不了 100K 真实请求的速度，且空闲期 probe 占请求大头，混入会污染选举、
     *  白烧免费配额预算。 */
    public void recordSuccess(String providerId, String model, String channelId, Long ttftMs,
                              Long tokensIn, Long tokensOut, boolean estimated, String client, long totalMs) {
        boolean probe = isolatedClient(client);
        EntryRuntime er = entry(providerId, model);
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        double alpha = d.getEwmaAlpha();
        synchronized (er) {
            if (!probe) {
                // 大上下文（输入>10K token）的 TTFT 不进 EWMA：首字慢主要是预填充/排队开销，
                // 不代表常规请求速度，混入会把快模型永久压出候选（吞吐样本照记——出活速度是真实能力）
                // ttftMs=null（非流式无首字样本）跳过速度样本：旧调用方注入合成 500ms 把慢候选洗白成中速
                if (ttftMs != null && (tokensIn == null || tokensIn <= LARGE_CONTEXT_TOKENS)) {
                    er.ewmaTtftMs = er.ewmaTtftMs < 0 ? ttftMs : alpha * ttftMs + (1 - alpha) * er.ewmaTtftMs;
                    // 同时进最近样本环形窗：中位数=抗尖峰的"典型首字速度"（分摊抽签门槛用它）
                    if (er.recentTtftWin == null) er.recentTtftWin = new long[8];
                    er.recentTtftWin[er.recentTtftIdx] = ttftMs;
                    er.recentTtftIdx = (er.recentTtftIdx + 1) % er.recentTtftWin.length;
                    if (er.recentTtftCount < er.recentTtftWin.length) er.recentTtftCount++;
                    recomputeRecentMedian(er);
                }
                // 吞吐因子：出 token/秒（按总时长；无 usage/时长异常不动，防离谱样本污染）
                if (tokensOut != null && tokensOut > 0 && totalMs > 0) {
                    double tps = tokensOut * 1000.0 / totalMs;
                    if (tps > 0 && tps < 1000) er.ewmaTokensPerSec =
                            er.ewmaTokensPerSec < 0 ? tps : alpha * tps + (1 - alpha) * er.ewmaTokensPerSec;
                }
            }
            if (er.coolDownUntil > 0 && er.halfOpenProbe) {
                // 半开探测成功 → 关闭熔断（探测唯一允许的熔断状态写入：恢复方向）
                er.coolDownUntil = 0;
                er.backoffMs = 0;
                er.halfOpenProbe = false;
                er.halfOpenBusy = false;
                log.info("渠道 {} × {} 半开探测成功，熔断恢复", providerId, model);
            }
            if (probe) {
                // 探测成功不清真实流量的连续失败计数：否则空闲期 ping 通一次
                // 就把累积的失败抹掉，间歇性抽风的模型永远攒不够熔断阈值
                return;
            }
            er.consecutiveFailures = 0;
        }
        addUsage(providerId, model, channelId, tokensIn, tokensOut, client);
        // 自适应限流学习：成功也计入滚动窗口用量（成功也消耗该家配额）
        long tIn = tokensIn == null ? 0 : tokensIn;
        long tOut = tokensOut == null ? 0 : tokensOut;
        rateLimiter.observe(providerId, model, channelId, tIn + tOut);
    }

    /** 输出质量软失败：截断/空回复/JSON 不合法。计入质量维度（不影响硬熔断），评分 Q 因子降权。
     *  成功清零，与 consecutiveFailures 复用同一个渐进式计数：连续软失败越多越该避开。 */
    public void recordQualityIssue(String providerId, String model) {
        EntryRuntime er = entry(providerId, model);
        synchronized (er) {
            er.qualityIssues++;
        }
    }

    /** 输出质量正常（完整、非空、合法）：质量计数清零 */
    public void recordQualityOk(String providerId, String model) {
        EntryRuntime er = entry(providerId, model);
        synchronized (er) {
            er.qualityIssues = 0;
        }
    }

    /** 该 provider×model 的软失败计数（诊断展示用） */
    public int qualityIssues(String providerId, String model) {
        return entry(providerId, model).qualityIssues;
    }

    /**
     * 早停嫌疑判定（由网关两个端点的输出质量反馈调用，异常绝不干扰主流程）：
     * - 响应真实发起工具调用 → 解除该 client×模型 的嫌疑（健康行为）
     * - 带 tools 请求 + 本请求经历过压缩 + 无 tool_calls + 回复 <24 字符 → 记 10 分钟惩罚。
     *   三个条件缺一不可：不压缩的正常短答（"好的"确认）与无工具闲聊都不会误伤，
     *   只有"压缩后 agent 等工具续跑却只收到一句话"这一高危组合才被标记。
     */
    public void judgeEarlyStop(String client, String providerId, String model, String compressTag,
                               boolean hasTools, boolean toolCalls, int contentChars) {
        try {
            if (client == null || client.isBlank() || isolatedClient(client)) return;
            // #33：真实流量（非隔离、走到此输出质量反馈即 status=OK 完成）带 tools 样本累计
            recordToolSample(providerId, model, hasTools, toolCalls);
            String key = earlyStopKey(client, providerId, model);
            if (toolCalls) {
                if (earlyStopPenalty.remove(key) != null) {
                    log.info("[早停] client={} × {}/{} 已真实发起工具调用，解除早停嫌疑惩罚", client, providerId, model);
                }
                return;
            }
            boolean compressed = compressTag != null
                    && (compressTag.contains("-ok") || compressTag.startsWith("hard-truncate"));
            if (hasTools && compressed && contentChars < EARLY_STOP_MAX_CHARS) {
                earlyStopPenalty.put(key, System.currentTimeMillis() + EARLY_STOP_TTL_MS);
                if (earlyStopPenalty.size() > 512) {
                    long now = System.currentTimeMillis();
                    earlyStopPenalty.entrySet().removeIf(en -> en.getValue() < now);
                }
                log.warn("[早停] client={} 在 {}/{} 上压缩后仅回 {} 字符且无工具调用，疑似静默早停，10 分钟内该组合带工具请求额外降权",
                        client, providerId, model, contentChars);
            }
        } catch (Exception ignore) {
        }
    }

    private static String twKey(String providerId, String model) {
        return providerId + "\n" + model;
    }

    /** #33 toolWeak 样本累计（幂等、异常绝不打断主流程）：
     *  - hasTools 的真实完成请求 → toolsRequestsSeen++
     *  - 一旦回过 tool_calls → toolCallsEverSeen=true（永久免疫建议）
     *  - 已标 toolWeak 的模型又回了 tool_calls → 记一次"矛盾"（反证展示，由人判断是否摘标） */
    public void recordToolSample(String providerId, String model, boolean hasTools, boolean toolCalls) {
        try {
            if (providerId == null || model == null) return;
            String k = twKey(providerId, model);
            if (toolCalls) {
                toolCallsEverSeen.put(k, Boolean.TRUE);
                if (configService.snapshot().effectiveToolWeak(providerId, model))
                    toolWeakContradictions.computeIfAbsent(k, x -> new AtomicInteger()).incrementAndGet();
            }
            if (hasTools)
                toolsRequestsSeen.computeIfAbsent(k, x -> new AtomicInteger()).incrementAndGet();
        } catch (Exception ignore) {
        }
    }

    /** #33 toolWeak 建议清单：达样本阈值且从未回 tool_calls、且当前未标 toolWeak 的模型。
     *  仅返回供人审，调用方（管理页一键应用）才会写回 yaml——本服务绝不自动打标。 */
    public Map<String, Object> toolWeakSuggestions() {
        ConfigService.Snapshot snap = configService.snapshot();
        int threshold = snap.config.getDefaults().getToolWeakSampleThreshold();
        if (threshold <= 0) threshold = 20;
        List<Map<String, Object>> suggestions = new ArrayList<>();
        for (var en : toolsRequestsSeen.entrySet()) {
            String k = en.getKey();
            int seen = en.getValue().get();
            if (seen < threshold) continue;
            if (Boolean.TRUE.equals(toolCallsEverSeen.get(k))) continue;
            int i = k.indexOf('\n');
            String provider = k.substring(0, i), model = k.substring(i + 1);
            if (snap.effectiveToolWeak(provider, model)) continue;   // 已打标不再建议
            suggestions.add(new java.util.LinkedHashMap<>(Map.of(
                    "provider", provider, "model", model, "samples", seen, "threshold", threshold)));
        }
        suggestions.sort((a, b) -> Integer.compare((int) b.get("samples"), (int) a.get("samples")));
        // 反证清单：已标 toolWeak 却又回了 tool_calls 的模型（不自动摘标，只提示）
        List<Map<String, Object>> contradictions = new ArrayList<>();
        for (var en : toolWeakContradictions.entrySet()) {
            int i = en.getKey().indexOf('\n');
            String provider = en.getKey().substring(0, i), model = en.getKey().substring(i + 1);
            if (en.getValue().get() > 0 && snap.effectiveToolWeak(provider, model))
                contradictions.add(new java.util.LinkedHashMap<>(Map.of(
                        "provider", provider, "model", model, "toolCallsSinceMark", en.getValue().get())));
        }
        return Map.of("threshold", threshold, "suggestions", suggestions, "contradictions", contradictions);
    }


    /** 请求失败：熔断计数（5xx 与超时）；429 走冷却快通道不触发熔断。失败也计请求次数（按工具统计完整性）。
     *  client=probe 的巡检失败不进主熔断（探测抖动不该把真实可用的模型打出候选池），
     *  但 429 的 Key 级冷却保留——那是上游真实配额信号，冷却该 Key 保护后续流量。 */
    public void recordFailure(String providerId, String model, String channelId, int httpStatus, String error,
                              String client) {
        boolean probe = isolatedClient(client);
        if (!probe && client != null && !client.isBlank()) addRequestCount(providerId, model, channelId, client);
        recordFailure(providerId, model, channelId, httpStatus, error, probe);
    }

    /** 请求失败：熔断计数（5xx 与超时）；429 走冷却快通道不触发熔断 */
    public void recordFailure(String providerId, String model, String channelId, int httpStatus, String error) {
        recordFailure(providerId, model, channelId, httpStatus, error, false);
    }

    private void recordFailure(String providerId, String model, String channelId, int httpStatus, String error,
                               boolean probe) {
        ConfigService.Snapshot snap = configService.snapshot();
        GatewayConfig.Defaults d = snap.config.getDefaults();
        long now = System.currentTimeMillis();

        // 失败也计入限流学习的滚动窗口（请求本身消耗配额：无论成败，向该家发起的
        // 请求次数都要进窗口，否则频繁 429 时窗口 requests 被低估，压力评估偏松，
        // 路由继续压量却不知该家已到顶）。token 记 0（失败拿不到 usage）。
        // 探测不进气压窗口：probe 是网关自造流量，不该影响对真实配额的评估。
        if (!probe) rateLimiter.observe(providerId, model, channelId, 0);

        // #9：先语义分类（覆盖"文字型限流"——上游把限流塞进 200/500 body、只露 code 1305/tpm-rpm 等，
        // 老逻辑因不带 HTTP 429 直接走熔断，下个请求继续撞同一面墙）。分类只依赖 (status, error)。
        UpstreamErrorClassifier.ErrorClass ec = UpstreamErrorClassifier.classify(httpStatus, error);

        if (ec == UpstreamErrorClassifier.ErrorClass.QUOTA
                || ec == UpstreamErrorClassifier.ErrorClass.RATE_LIMIT) {
            // Key 级冷却（Retry-After 由 forward 层 applyRateLimitCooldown 另行拉长）。
            // 配额耗尽 ≠ 瞬时 TPM/RPM 限流：前者短期不恢复，按 quotaCooldownMs 长冷却下线，
            // 避免每个请求都撞墙烧一次 attempt；后者短冷却 + 自适应学习。
            ChannelRuntime cr = channel(providerId, channelId);
            boolean quota = ec == UpstreamErrorClassifier.ErrorClass.QUOTA;
            long cd = quota ? d.getQuotaCooldownMs() : d.getRateLimitCooldownMs();
            cr.coolDownUntil = now + cd;
            cr.lastRateLimitedAt = now;   // 记录限流时刻：冷却到期后仍短暂降权，防连发立刻打回
            if (!probe) {
                // 自适应限流学习：把当前窗口用量采样为该家的经验上限（越用越准）。
                // 探测跳过：probe 不 observe，窗口用量失真，采样会学出虚假低上限
                rateLimiter.recordRateLimit(providerId, model, channelId, 0);
            }
            log.warn("渠道 {}/{} {}（http {}），Key {} 冷却 {}ms{}", providerId, model,
                    quota ? "配额耗尽" : "限流", httpStatus, channelId, cd, probe ? "（探测发现）" : "");
            return;
        }
        if (ec == UpstreamErrorClassifier.ErrorClass.AUTH) {
            // 鉴权失败是 Key 的问题不是模型/渠道健康度：只冷却该 Key，不推动熔断计数
            //（同一 Key 对所有模型都会 401，模型侧熔断会把整家误伤）
            ChannelRuntime cr = channel(providerId, channelId);
            cr.coolDownUntil = Math.max(cr.coolDownUntil, now + d.getQuotaCooldownMs());
            log.warn("渠道 {}/{} {} 鉴权失败，Key {} 冷却 {}ms{}", providerId, model, httpStatus, channelId,
                    d.getQuotaCooldownMs(), probe ? "（探测发现）" : "");
            return;
        }
        if (ec == UpstreamErrorClassifier.ErrorClass.MODEL_NOT_SUPPORTED) {
            // 上游确实没有该模型（配置/目录漂移）：短期豁免该家该模型，避免每个请求都白烧一次 attempt
            // 去撞它。不碰渠道健康、不推模型熔断（这不是"故障"，是"没有"），到期半开再验。
            EntryRuntime er = entry(providerId, model);
            synchronized (er) {
                er.lastError = error;
                if (!probe) {
                    long exempt = Math.max(d.getRateLimitCooldownMs(), 60_000);
                    er.coolDownUntil = Math.max(er.coolDownUntil, now + exempt);
                }
            }
            log.warn("渠道 {}/{} 上游不支持该模型，短期豁免（不推熔断）{}", providerId, model,
                    probe ? "（探测发现，仅留痕）" : "");
            return;
        }
        if (ec == UpstreamErrorClassifier.ErrorClass.CONTEXT_TOO_LONG
                || ec == UpstreamErrorClassifier.ErrorClass.REASONING_PASSBACK) {
            // 上下文超长/参数不合法：请求内容问题，与渠道健康无关，只留痕不熔断。
            // REASONING_PASSBACK 同档：那是"该家默认开思考 + 历史没带思考内容"，换家也一样，
            // 惩罚渠道健康度只会把分摊打乱（网关已在转发层自愈：回填思考内容或关思考重试）。
            EntryRuntime erx = entry(providerId, model);
            synchronized (erx) { erx.lastError = error; }
            return;
        }
        if (httpStatus >= 400 && httpStatus < 500 && httpStatus != 429) {
            // 其余 4xx（400 参数、404 其它、422…）同样按"请求内容不适配"处理：只留痕不熔断。
            // 实测教训：一个 152K 大请求 400 三次就把好模型打熔断，后续正常请求全被赶走
            EntryRuntime er4 = entry(providerId, model);
            synchronized (er4) { er4.lastError = error; }
            return;
        }
        if (probe) {
            // 巡检失败只留痕（lastRound/request_log），不推动主熔断状态机：
            // 空闲期 probe 占请求大头，一次网络抖动就把真实可用的模型打出候选池是净伤害。
            // 熔断恢复验证仍走成功侧的半开路径（tryEnterHalfOpen + recordSuccess）。
            EntryRuntime per = entry(providerId, model);
            synchronized (per) { per.lastError = error; }   // 仅供诊断展示
            return;
        }
        EntryRuntime er = entry(providerId, model);
        synchronized (er) {
            er.halfOpenBusy = false;   // 半开请求已出结果：放槽（未达阈值则下一条请求重试验证）
            er.consecutiveFailures++;
            er.lastError = error;
            if (er.consecutiveFailures >= d.getCircuitBreakerThreshold()) {
                long backoff = er.backoffMs <= 0 ? d.getCircuitBreakerBaseCooldownMs() : er.backoffMs * 2;
                backoff = Math.min(backoff, d.getCircuitBreakerMaxCooldownMs());
                er.backoffMs = backoff;
                er.coolDownUntil = now + backoff;
                er.halfOpenProbe = false;
                log.warn("渠道 {} × {} 连续失败 {} 次，熔断 OPEN 冷却 {}ms", providerId, model, er.consecutiveFailures, backoff);
            }
        }
    }

    /** Retry-After 带来的精确冷却（秒） */
    public void recordRateLimitWithRetryAfter(String providerId, String channelId, long retryAfterSeconds) {
        ChannelRuntime cr = channel(providerId, channelId);
        long until = System.currentTimeMillis() + retryAfterSeconds * 1000;
        if (until > cr.coolDownUntil) cr.coolDownUntil = until;
    }

    /** 多 Key 轮转记账：请求实际走某 Key 时记录时间戳与计数。
     *  排序按 lastUsedAt（刚用过的排后）实现严格交替——不管历史流量多少，
     *  两把 Key 永远一单一单轮流接单。 */
    public void recordChannelUsed(String providerId, String channelId) {
        ChannelRuntime cr = channel(providerId, channelId);
        cr.lastUsedAt = System.currentTimeMillis();
        cr.rotationCounter++;
    }

    /** 冷却到期后第一次被选中时置半开标记（放行单个探测请求） */
    public boolean tryEnterHalfOpen(String providerId, String model) {
        EntryRuntime er = entry(providerId, model);
        synchronized (er) {
            long now = System.currentTimeMillis();
            if (er.coolDownUntil > 0 && now >= er.coolDownUntil && !er.halfOpenProbe) {
                er.halfOpenProbe = true;
                return true;
            }
        }
        return false;
    }

    /** 抢占半开槽：同一时刻仅一条在途请求可去验证恢复；持有超 60s 未回调（断连/漏记）视为失效自动放槽 */
    private boolean tryClaimHalfOpen(EntryRuntime er, long now) {
        synchronized (er) {
            if (!er.halfOpenBusy || now - er.halfOpenSince > 60_000) {
                er.halfOpenBusy = true;
                er.halfOpenSince = now;
                return true;
            }
            return false;
        }
    }

    private void addUsage(String providerId, String model, String channelId, Long tokensIn, Long tokensOut,
                          String client) {
        long in = tokensIn == null ? 0 : tokensIn;
        long out = tokensOut == null ? 0 : tokensOut;
        if (in == 0 && out == 0) return;
        UsageCounter entryC = usage.computeIfAbsent(entryKey(providerId, model), k -> new UsageCounter());
        entryC.tokensIn.addAndGet(in);
        entryC.tokensOut.addAndGet(out);
        entryC.requests.incrementAndGet();
        if (channelId != null) {
            UsageCounter chC = usage.computeIfAbsent(channelKey(providerId, channelId), k -> new UsageCounter());
            chC.tokensIn.addAndGet(in);
            chC.tokensOut.addAndGet(out);
            // Key 维度的请求数必须与 provider×model 维度同起同落：只加 token 不加 requests，
            // 界面"今日 N 次"会恒显示 0，而"今日 token"有数——两个数摆在一起就是自相矛盾的假数据
            chC.requests.incrementAndGet();
        }
        // 异步落库（当日聚合；'' 桶 = entry 总额，工具桶单列）
        String cl = client == null || client.isBlank() ? "unknown" : client;
        dao.upsertUsage(currentDay, providerId, model, "", 1, in, out);
        dao.upsertUsage(currentDay, providerId, model, cl, 1, in, out);
    }

    /** 计一次请求（无 token；失败/无 usage 时也保证"请求次数"维度完整）。
     *  channelId 非空时同步计入该 Key，保持 provider×model 与 provider×Key 两个维度的请求数同口径。 */
    public void addRequestCount(String providerId, String model, String channelId, String client) {
        UsageCounter entryC = usage.computeIfAbsent(entryKey(providerId, model), k -> new UsageCounter());
        entryC.requests.incrementAndGet();
        if (channelId != null && !channelId.isBlank()) {
            UsageCounter chC = usage.computeIfAbsent(channelKey(providerId, channelId), k -> new UsageCounter());
            chC.requests.incrementAndGet();
        }
        String cl = client == null || client.isBlank() ? "unknown" : client;
        dao.upsertUsage(currentDay, providerId, model, "", 1, 0, 0);
        dao.upsertUsage(currentDay, providerId, model, cl, 1, 0, 0);
    }

    // ================= 并发占用 =================

    public void acquireInflight(String providerId, String model) {
        inflightEntry.computeIfAbsent(entryKey(providerId, model), k -> new AtomicInteger())
                .incrementAndGet();
        inflightProvider.computeIfAbsent(providerId, k -> new AtomicInteger()).incrementAndGet();
    }

    public void releaseInflight(String providerId, String model) {
        AtomicInteger e = inflightEntry.get(entryKey(providerId, model));
        if (e != null) e.decrementAndGet();
        AtomicInteger p = inflightProvider.get(providerId);
        if (p != null) p.decrementAndGet();
    }

    public int inflight(String providerId, String model) {
        AtomicInteger e = inflightEntry.get(entryKey(providerId, model));
        return e == null ? 0 : e.get();
    }

    /** #13 全局在途请求数（各 provider 在途之和）：探测空闲门控据此判断"当前有无真实流量" */
    public int totalInflight() {
        int sum = 0;
        for (AtomicInteger p : inflightProvider.values()) sum += Math.max(0, p.get());
        return sum;
    }

    // ================= 预算重置 / 快照 =================

    public void resetBudget() {
        usage.clear();
        try {
            dao.getDb().syncWrite("DELETE FROM usage_daily WHERE day = ?", currentDay);
        } catch (Exception ignore) {
        }
        log.info("额度已手动重置");
    }

    /** 供 admin 展示的运行时视图 */
    public Map<String, Object> runtimeView() {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Map<String, Object>> entryRows = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, EntryRuntime> e : entries.entrySet()) {
            String[] pk = e.getKey().split("\n", 2);
            EntryRuntime er = e.getValue();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("provider", pk[0]);
            row.put("model", pk[1]);
            row.put("ewmaTtftMs", er.ewmaTtftMs < 0 ? null : Math.round(er.ewmaTtftMs));
            row.put("recentTtftMedian", er.recentTtftMedian < 0 ? null : Math.round(er.recentTtftMedian));
            row.put("ewmaTokensPerSec", er.ewmaTokensPerSec < 0 ? null : Math.round(er.ewmaTokensPerSec * 10) / 10.0);
            row.put("consecutiveFailures", er.consecutiveFailures);
            // OPEN=冷却未到期；HALF_OPEN=到期后半开验证窗口（放行槽占用/待验证）；CLOSED=正常
            row.put("state", er.coolDownUntil > now ? "OPEN"
                    : (er.halfOpenProbe || er.halfOpenBusy ? "HALF_OPEN" : "CLOSED"));
            row.put("coolDownUntil", er.coolDownUntil);
            row.put("lastError", er.lastError);
            entryRows.add(row);
        }
        out.put("entries", entryRows);
        List<Map<String, Object>> chRows = new ArrayList<>();
        for (Map.Entry<String, ChannelRuntime> e : channels.entrySet()) {
            String[] ck = e.getKey().split("\n", 2);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("provider", ck[0]);
            row.put("channel", ck[1]);
            row.put("state", e.getValue().coolDownUntil > now ? "COOLING" : "AVAILABLE");
            row.put("coolDownUntil", e.getValue().coolDownUntil);
            row.put("lastRateLimitedAt", e.getValue().lastRateLimitedAt);
            chRows.add(row);
        }
        out.put("channels", chRows);
        // 早停嫌疑在惩罚窗口内的 client×模型组合（诊断透明：为什么某模型突然排后了）
        List<Map<String, Object>> esRows = new ArrayList<>();
        for (Map.Entry<String, Long> e : earlyStopPenalty.entrySet()) {
            if (e.getValue() <= now) continue;
            String[] p = e.getKey().split("\n", 3);
            if (p.length != 3) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("client", p[0]);
            row.put("provider", p[1]);
            row.put("model", p[2]);
            row.put("remainingMs", e.getValue() - now);
            esRows.add(row);
        }
        out.put("earlyStopSuspects", esRows);
        return out;
    }

    /** 定期快照（60s 一次，重启恢复用） */
    public void persistSnapshot() {
        List<StatsDao.SnapshotRow> rows = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, EntryRuntime> e : entries.entrySet()) {
            String[] pk = e.getKey().split("\n", 2);
            EntryRuntime er = e.getValue();
            String state = er.coolDownUntil > now ? "OPEN" : "CLOSED";
            rows.add(new StatsDao.SnapshotRow(pk[0], "*", pk[1],
                    er.ewmaTtftMs < 0 ? null : er.ewmaTtftMs, state,
                    er.coolDownUntil, er.consecutiveFailures,
                    er.ewmaTokensPerSec < 0 ? null : er.ewmaTokensPerSec,
                    er.recentTtftMedian < 0 ? null : er.recentTtftMedian));
        }
        for (Map.Entry<String, ChannelRuntime> e : channels.entrySet()) {
            String[] ck = e.getKey().split("\n", 2);
            ChannelRuntime cr = e.getValue();
            rows.add(new StatsDao.SnapshotRow(ck[0], ck[1], "*", null,
                    cr.coolDownUntil > now ? "COOLING" : "AVAILABLE",
                    cr.coolDownUntil, 0, null, null));
        }
        if (!rows.isEmpty()) dao.saveRuntimeSnapshot(rows);
    }

    /** 启动恢复运行时快照 */
    public void restoreRuntime() {
        for (StatsDao.SnapshotRow r : dao.loadRuntimeSnapshots()) {
            if ("*".equals(r.channelId())) {
                EntryRuntime er = entry(r.providerId(), r.model());
                if (r.ewmaTtftMs() != null) er.ewmaTtftMs = r.ewmaTtftMs();
                if (r.ewmaTokensPerSec() != null) er.ewmaTokensPerSec = r.ewmaTokensPerSec();
                if (r.recentTtftMedian() != null) er.recentTtftMedian = r.recentTtftMedian();
                if (r.coolDownUntil() != null && r.coolDownUntil() > System.currentTimeMillis()) {
                    er.coolDownUntil = r.coolDownUntil();
                    er.consecutiveFailures = r.consecutiveFailures() == null ? 0 : r.consecutiveFailures();
                    er.backoffMs = Math.max(60_000, (r.coolDownUntil() - System.currentTimeMillis()));
                }
            } else if ("*".equals(r.model())) {
                ChannelRuntime cr = channel(r.providerId(), r.channelId());
                if (r.coolDownUntil() != null && r.coolDownUntil() > System.currentTimeMillis()) {
                    cr.coolDownUntil = r.coolDownUntil();
                }
            }
        }
    }

    public String today() { return currentDay; }

    /** 实测首字速度总览（loadShare 慢门槛微调依据）：取所有"有最近实测中位数"模型的
     *  中位数的中位数；建议慢门槛 = 该值 ×2（只排掉比典型速度慢一倍的），钳到 1000~60000 并取整 500。
     *  仅统计有真实流量样本的条目（probe 不进中位数窗，天然不含）。 */
    public Map<String, Object> ttftStats() {
        List<Long> meds = new ArrayList<>();
        for (EntryRuntime er : entries.values()) {
            if (er.recentTtftMedian >= 0) meds.add(Math.round(er.recentTtftMedian));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("samples", meds.size());
        if (meds.isEmpty()) return out;
        Collections.sort(meds);
        long med = meds.get(meds.size() / 2);
        long sug = Math.round(med * 2 / 500.0) * 500;
        out.put("medianMs", med);
        out.put("suggestedSlowTtftMs", Math.max(1000, Math.min(60000, sug)));
        return out;
    }

    /** 自适应限流学习诊断：看到的各 provider 经验上限 + 当前用量（帮助理解为何避让某家） */
    public Map<String, Object> rateLimitView() { return rateLimiter.view(); }

    /** 手动重置某家的自适应限流学习（重新从零学），返回是否确实存在并清除 */
    public boolean resetRateLimit(String providerId) { return rateLimiter.reset(providerId); }

    /** 该 provider×model 学到的单请求最大安全输出 token（截断事件学习；0=未学过不钳制） */
    public int safeMaxOutputTokens(String providerId, String model) { return rateLimiter.safeMaxOutputTokens(providerId, model); }

    /** 记录一次输出截断（该家单请求最大输出上限样本）；requestedMaxTokens≤0 表示用户未设预算 */
    public void recordOutputTruncation(String providerId, String model, int completionTokens, int requestedMaxTokens) {
        rateLimiter.recordOutputTruncation(providerId, model, completionTokens, requestedMaxTokens);
    }

    // ================= 候选与打分结果模型 =================

    public record Candidate(String providerId, String model, String channelId, int weight, Integer groupPriority) {}

    public static class Scored {
        public final Candidate candidate;
        public final double score;
        public final boolean blocked;   // 预算耗尽/并发满（兜底放行时使用）
        /** 仅评分留痕（预览/采样落库）时非空：本分的每个因子取值，界面用来解释"这一分怎么来的"。
         *  热路径传 null，不额外分配。 */
        public final Map<String, Double> factors;
        Scored(Candidate c, double score, boolean blocked) { this(c, score, blocked, null); }
        Scored(Candidate c, double score, boolean blocked, Map<String, Double> factors) {
            this.candidate = c;
            this.score = score;
            this.blocked = blocked;
            this.factors = factors;
        }
    }

    /**
     * 一次 rank() 决策的完整快照——回答"这次工具调用为什么给了他、没给别人"。
     *
     * <p>热路径默认传 null（零开销）；只有评分沙盘预览、或按采样率开启留痕时才收集。
     * rows = 进入排序的候选（含分数、因子、状态、抽签席与应得份额、是否被选中）；
     * filtered = 连分都没打的候选——它们是"为什么没分摊到我"的主要盲区，四类门控
     * （渠道禁用 / 临时停用 / 档位不符 / 上下文超限）在 rank() 里原本是裸 continue。
     */
    public static final class RankTrace {
        public long ts = System.currentTimeMillis();
        public String traceId;
        public String requestedModel;
        public String client;
        public String endpoint;
        public String strategy;
        public boolean critical;
        public String category;
        public boolean hasTools;
        public Integer inputTokens;
        public boolean loadShare;
        public int bandSize;
        public long slowTtftMs;
        /** true = 管理台"评分沙盘"主动重放，不代表真实发生的一次调用 */
        public boolean preview;
        public final List<Row> rows = new ArrayList<>();
        public final List<Row> filtered = new ArrayList<>();
        public String pickedProvider;
        public String pickedModel;
        public String pickReason;
        /** 采样落库时为控制体积只存了前若干行候选，这里记原始行数（界面须说明"仅存前 N 行"） */
        public Integer rowsTruncated;

        public Row newRow(Candidate c, double score, Map<String, Double> factors) {
            Row r = new Row();
            r.provider = c.providerId();
            r.model = c.model();
            r.channel = c.channelId();
            r.score = score;
            r.factors = factors;
            rows.add(r);
            return r;
        }

        public Map<String, Object> toJson() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ts", ts);
            m.put("traceId", traceId);
            m.put("requestedModel", requestedModel);
            m.put("client", client);
            m.put("endpoint", endpoint);
            m.put("strategy", strategy);
            m.put("critical", critical);
            m.put("category", category);
            m.put("hasTools", hasTools);
            m.put("inputTokens", inputTokens);
            m.put("loadShare", loadShare);
            m.put("bandSize", bandSize);
            m.put("slowTtftMs", slowTtftMs);
            m.put("preview", preview);
            m.put("pickedProvider", pickedProvider);
            m.put("pickedModel", pickedModel);
            m.put("pickReason", pickReason);
            List<Object> rs = new ArrayList<>();
            for (Row r : rows) rs.add(r.toJson(false));
            m.put("rows", rs);
            List<Object> fs = new ArrayList<>();
            for (Row r : filtered) fs.add(r.toJson(true));
            m.put("filtered", fs);
            return m;
        }

        /** 从落库的 JSON 还原（只回读界面要用的字段；因子表原样带出） */
        @SuppressWarnings("unchecked")
        public static RankTrace fromJson(Map<String, Object> m) {
            RankTrace t = new RankTrace();
            t.ts = (long) num(m.get("ts"), System.currentTimeMillis());
            t.traceId = str(m.get("traceId"));
            t.requestedModel = str(m.get("requestedModel"));
            t.client = str(m.get("client"));
            t.endpoint = str(m.get("endpoint"));
            t.strategy = str(m.get("strategy"));
            t.critical = Boolean.TRUE.equals(m.get("critical"));
            t.category = str(m.get("category"));
            t.hasTools = Boolean.TRUE.equals(m.get("hasTools"));
            Object it = m.get("inputTokens");
            t.inputTokens = it instanceof Number n ? n.intValue() : null;
            t.loadShare = Boolean.TRUE.equals(m.get("loadShare"));
            t.bandSize = (int) num(m.get("bandSize"), 0);
            t.slowTtftMs = (long) num(m.get("slowTtftMs"), 0);
            t.preview = Boolean.TRUE.equals(m.get("preview"));
            t.pickedProvider = str(m.get("pickedProvider"));
            t.pickedModel = str(m.get("pickedModel"));
            t.pickReason = str(m.get("pickReason"));
            readRows(m.get("rows"), t.rows, false);
            readRows(m.get("filtered"), t.filtered, true);
            Object tr = m.get("rowsTruncated");
            if (tr instanceof Number n) t.rowsTruncated = n.intValue();
            return t;
        }

        @SuppressWarnings("unchecked")
        private static void readRows(Object list, List<Row> dest, boolean filtered) {
            if (!(list instanceof Collection<?> col)) return;
            for (Object o : col) {
                if (!(o instanceof Map<?, ?> mm)) continue;
                Map<String, Object> m = (Map<String, Object>) mm;
                Row r = new Row();
                r.provider = str(m.get("provider"));
                r.model = str(m.get("model"));
                r.channel = str(m.get("channel"));
                r.score = num(m.get("score"), 0);
                r.state = str(m.get("state"));
                r.inBand = Boolean.TRUE.equals(m.get("inBand"));
                r.bandOut = str(m.get("bandOut"));
                r.share = num(m.get("share"), 0);
                r.picked = Boolean.TRUE.equals(m.get("picked"));
                r.note = str(m.get("note"));
                r.reason = str(m.get("reason"));
                if (m.get("factors") instanceof Map<?, ?> fm) {
                    Map<String, Double> f = new LinkedHashMap<>();
                    fm.forEach((k, v) -> { if (v instanceof Number n) f.put(String.valueOf(k), n.doubleValue()); });
                    r.factors = f;
                }
                dest.add(r);
            }
        }

        private static String str(Object o) { return o == null ? null : String.valueOf(o); }
        private static double num(Object o, double dflt) { return o instanceof Number n ? n.doubleValue() : dflt; }

        /** 单个候选在本次决策里的位置 */
        public static final class Row {
            public String provider;
            public String model;
            public String channel;
            public double score;
            public Map<String, Double> factors;
            /** OK=正常参与 / COOLING=熔断或限流冷却 / BLOCKED=预算耗尽或并发满 / FILTERED=门控直接出局（只在 filtered 列表里） */
            public String state = "OK";
            public boolean inBand;      // 进入分摊抽签席
            /** 未进抽签席的原因码（BAND_* ；界面按码分类计数，不靠人肉读 note 文案） */
            public String bandOut;
            public double share;        // 席内应得份额（归一化后的 √(score/max) 权重，0=不在席内）
            public boolean picked;      // 本次主选
            public String note;         // 人话说明（为什么冷却/为什么兜底/为什么没进席）
            public String reason;       // 仅 filtered：原因码

            public Map<String, Object> toJson(boolean isFiltered) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("provider", provider);
                m.put("model", model);
                m.put("channel", channel);
                if (!isFiltered) {
                    m.put("score", round(score));
                    m.put("state", state);
                    m.put("inBand", inBand);
                    if (inBand) m.put("share", round(share));
                    if (bandOut != null) m.put("bandOut", bandOut);
                    m.put("picked", picked);
                    if (factors != null) {
                        Map<String, Object> f = new LinkedHashMap<>();
                        factors.forEach((k, v) -> f.put(k, round(v)));
                        m.put("factors", f);
                    }
                } else {
                    m.put("reason", reason);
                }
                if (note != null) m.put("note", note);
                return m;
            }
        }
    }

    /** 界面读数够用即可：因子和分数保留 4 位，避免落库 JSON 被 1e-17 尾数撑大 */
    private static double round(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return v;
        return Math.round(v * 10000.0) / 10000.0;
    }
}
