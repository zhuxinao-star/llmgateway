package com.gw.probe;

import com.gw.catalog.CatalogService;
import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.forward.ForwardEngine;
import com.gw.policy.PolicyService;
import com.gw.policy.UpstreamErrorClassifier;
import com.gw.policy.UpstreamErrorClassifier.ErrorClass;
import com.gw.stats.StatsService;
import com.gw.store.StatsDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 模型自动停用/恢复器：把"手动停用失效模型"变成定时自愈。
 *
 * 用户诉求（2026-09-19）：上游随时增删模型，列表里查不到 + 调用确实报"模型不存在/余额不足"的，
 * 希望自动停用；但像智谱几家"列表查不到却能调"的模型绝不能误停。
 *
 * 与 ProbeScheduler 的关键区别：本器写路由配置（停用/恢复），探测只为决策服务，
 * 结果以 client='reconcile' 隔离（PolicyService.ISOLATED_CLIENTS）——不进熔断/EWMA/预算/限流学习。
 *
 * 每轮流程：
 * 1. 实时拉各启用供应商的 /models 列表（fetchUpstreamModelsNow，非缓存）。拉失败=列表未知。
 * 2. 取 request_log 近 24h 证据：按 (provider,model) 聚合 MODEL_NOT_SUPPORTED / QUOTA 失败数（排除 probe/selfcheck/reconcile 行）。
 * 3. 筛候选（只探 free 档、未停用、未被排除探测/图像类跳过的模型）：
 *    "确认探测"的触发 = 列表里没有 OR 近窗口失败数 ≥ reconcileMinFailures。缺列表不等于停——
 *    必须补发一条确认探测，由它的结论决定停不停（保护"查不到但能调"的模型）。
 * 4. 恢复：被本器自动停过（autoDisabled=true）的模型若重新出现在列表 → 直接启用（免探测省额度）；
 *    仍不在列表的，探测成功也启用。人工停用（autoDisabled≠true）永不自动恢复。
 *    余额类是内存态临时停用（PolicyService.tempDisabled），到期或被确认探测成功即解除。
 * 5. 决策见 {@link #decide}。配置写入（永久停用 + 恢复）在一轮末尾合并成一次 persist 落盘。
 *
 * 安全默认：总开关 autoDisableEnabled=false（关）；只走 free 档；每轮探测请求数封顶 reconcileProbeBudget；
 * 空闲门控让路真实流量（同 ProbeScheduler，延后超 MAX_DEFER_MS 强制执行一轮）。
 */
@Service
public class ModelReconciler {

    private static final Logger log = LoggerFactory.getLogger(ModelReconciler.class);

    /** 有真实流量时本轮让路；累计延后超此时长强制执行（防半开恢复/停用饿死）。 */
    private static final long MAX_DEFER_MS = 10 * 60_000L;
    /** 被动证据回看窗口：近 24h 的失败计数用于"该不该给这个模型补确认探测"。 */
    private static final long EVIDENCE_WINDOW_MS = 24 * 3600_000L;

    private final ConfigService configService;
    private final PolicyService policyService;
    private final ForwardEngine forwardEngine;
    private final StatsService statsService;
    private final CatalogService catalogService;
    private final StatsDao statsDao;

    /** 最近一轮动作留痕（管理页展示）：{provider, model, action, reason, at} */
    private final List<Map<String, Object>> lastActions = new ArrayList<>();
    private volatile String lastRoundAt = null;

    public ModelReconciler(ConfigService configService, PolicyService policyService,
                           ForwardEngine forwardEngine, StatsService statsService,
                           CatalogService catalogService, StatsDao statsDao) {
        this.configService = configService;
        this.policyService = policyService;
        this.forwardEngine = forwardEngine;
        this.statsService = statsService;
        this.catalogService = catalogService;
        this.statsDao = statsDao;
    }

    /** 决策动作（纯枚举，供 {@link #decide} 与单测共用）。 */
    public enum Action { NONE, PERMANENT_DISABLE, TEMP_DISABLE, RECOVER }

    @PostConstruct
    public void start() {
        Thread.ofVirtual().name("gw-reconcile-scheduler").start(this::loop);
    }

    private void loop() {
        try { Thread.sleep(100_000); } catch (InterruptedException e) { return; }  // 启动等 100s：等目录/统计就绪，且晚于 probe
        long lastAt = 0;
        long deferSince = 0;
        while (true) {
            try {
                GatewayConfig cfg;
                try {
                    cfg = configService.snapshot().config;
                } catch (IllegalStateException e) {
                    Thread.sleep(30_000);
                    continue;
                }
                if (!cfg.getDefaults().isAutoDisableEnabled()) {   // 总开关关：不扫描
                    Thread.sleep(60_000);
                    continue;
                }
                int intervalMin = cfg.getDefaults().getReconcileIntervalMinutes();
                if (intervalMin <= 0) {                            // 0=关闭调度
                    Thread.sleep(60_000);
                    continue;
                }
                long now = System.currentTimeMillis();
                if (now - lastAt >= intervalMin * 60_000L) {
                    int inflight = policyService.totalInflight();
                    boolean forced = deferSince != 0 && now - deferSince >= MAX_DEFER_MS;
                    if (inflight > 0 && !forced) {
                        if (deferSince == 0) deferSince = now;
                        log.debug("自动停用器延后本轮：在途 {} 条", inflight);
                    } else {
                        if (forced) log.info("自动停用器强制执行一轮：延后累计超 {} 分钟（在途 {} 条）",
                                MAX_DEFER_MS / 60000, inflight);
                        lastAt = now;
                        deferSince = 0;
                        runRound();
                    }
                }
                Thread.sleep(60_000L);
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                log.warn("自动停用器异常: {}", e.getMessage());
                try { Thread.sleep(30_000); } catch (InterruptedException ie) { return; }
            }
        }
    }

    /**
     * 纯决策：给定"确认探测结论 + 是否在上游列表 + 当前是否被本器自动停过"，产出该模型本轮动作。
     *
     * 铁律：absence-from-list 绝不单独停（智谱类"查不到但能调"必须存活）——停与恢都以确认探测结论为准；
     * 列表只用于恢复（重新收录即疑虑消除，免再烧一次探测）。非"模型不存在/余额"的失败（限流/超时/鉴权等）
     * 不作死活证据，一律不动手（fail-open，宁可漏停绝不误停）。
     *
     * @param probeRan     是否真的发出了确认探测（预算耗尽/无渠道=false → 绝不据此动手）
     * @param probeOk      探测是否成功产出（成功=模型活着）
     * @param probeClass   探测失败的语义分类（probeOk=true 时忽略）
     * @param inUpstreamList 该模型是否在实时上游列表里
     * @param currentlyAutoDisabled 该模型当前是否处于"被本器自动停用"态
     */
    public static Action decide(boolean probeRan, boolean probeOk, ErrorClass probeClass,
                                boolean inUpstreamList, boolean currentlyAutoDisabled) {
        if (currentlyAutoDisabled) {
            // 恢复优先：列表重新收录即恢复（免探测）；否则只有确认探测成功才恢复。
            if (inUpstreamList) return Action.RECOVER;
            if (probeRan && probeOk) return Action.RECOVER;
            return Action.NONE;                    // 仍不在列表且没探测过/没成功 → 维持停用
        }
        if (!probeRan) return Action.NONE;         // 没确认绝不动手
        if (probeOk) return Action.NONE;           // 能调通 → 什么都不做
        if (probeClass == ErrorClass.MODEL_NOT_SUPPORTED) return Action.PERMANENT_DISABLE;
        if (probeClass == ErrorClass.QUOTA) return Action.TEMP_DISABLE;
        return Action.NONE;                        // 其它失败：不可判模型死活，放过
    }

    /** 手动触发一轮（管理页按钮）：阻塞执行完返回本轮动作列表 */
    public List<Map<String, Object>> runRoundNow() {
        runRound();
        return List.copyOf(lastActions);
    }

    private void runRound() {
        lastActions.clear();
        lastRoundAt = LocalTime.now().withNano(0).toString();
        ConfigService.Snapshot snap = configService.snapshot();
        GatewayConfig.Defaults d = snap.config.getDefaults();
        int budget = Math.max(0, d.getReconcileProbeBudget());
        int minFailures = Math.max(1, d.getReconcileMinFailures());

        // 证据：近 24h (provider\nmodel) → {notFound, quota}
        Map<String, long[]> evidence = new HashMap<>();
        for (Map<String, Object> row : statsDao.recentModelOutcomes(System.currentTimeMillis() - EVIDENCE_WINDOW_MS)) {
            String k = row.get("provider") + "\n" + row.get("model");
            evidence.put(k, new long[]{asLong(row.get("notFound")), asLong(row.get("quota"))});
        }

        // 实时上游列表：providerId → 小写模型 id 集合；拉失败=null（列表未知，不据此停也不据此恢复）
        Map<String, Set<String>> lists = new LinkedHashMap<>();
        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            if (!p.isEnabled()) continue;
            try {
                Set<String> set = new HashSet<>();
                for (String id : catalogService.fetchUpstreamModelsNow(p))
                    if (id != null) set.add(id.toLowerCase());
                lists.put(p.getId(), set);
            } catch (Exception e) {
                lists.put(p.getId(), null);   // 列表未知
                log.debug("上游列表拉取失败 provider={}: {}", p.getId(), e.getMessage());
            }
        }

        List<Job> queue = new ArrayList<>();   // 待确认探测的候选（含恢复意图）

        // A. 恢复候选：被本器自动停用、现在重新出现在列表 → 立即恢复（免探测）；仍不在列表的排入探测队列
        List<String[]> recoverByList = new ArrayList<>();   // [pid, model]
        for (GatewayConfig.RoutingOverride ro : snap.config.getRoutingOverrides()) {
            if (!Boolean.TRUE.equals(ro.getAutoDisabled()) || !Boolean.FALSE.equals(ro.getEnabled())) continue;
            String pid = ro.getProviderId(), m = ro.getModel();
            Set<String> set = lists.get(pid);
            boolean inList = set != null && m != null && set.contains(m.toLowerCase());
            if (inList) recoverByList.add(new String[]{pid, m});
            else if (budgetRemaining(queue, budget)) queue.add(new Job(pid, m, true));
        }

        // B. 停用候选：启用中的 free 档模型，"列表里没有"或"近窗口失败达阈"才补确认探测
        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            if (!p.isEnabled()) continue;
            Set<String> set = lists.get(p.getId());
            for (String m : p.getStaticModels()) {
                if (snap.excludedFromAutoAll(p.getId(), m)) continue;
                if (snap.effectiveExcludeFromProbe(p.getId(), m)) continue;
                if (snap.modelDisabled(p.getId(), m)) continue;
                // 只为"清理已下线模型"补一条确认 ping——真付费档（standard/premium）绝不为此花钱；
                // free/low（含免费提供商的低档）允许，且候选本就限定"上游列表已查不到/有失败证据"，
                // 在列表里的健康付费模型根本不会进到这里，成本封顶为"每个真下线模型一次"。
                String tier = snap.effectiveCostTier(p.getId(), m);
                if ("standard".equalsIgnoreCase(tier) || "premium".equalsIgnoreCase(tier)) continue;
                if (policyService.isTempDisabled(p.getId(), m)) continue;   // 已在临时停用中，另走探测恢复
                boolean inList = set != null && set.contains(m.toLowerCase());
                long[] ev = evidence.getOrDefault(p.getId() + "\n" + m, new long[]{0, 0});
                boolean hasEvidence = (ev[0] + ev[1]) >= minFailures;
                boolean absentFromList = set != null && !inList;   // 列表已知且确实没有才算"缺席"
                if (!absentFromList && !hasEvidence) continue;     // 既在列表又无失败证据 → 不打扰
                if (budgetRemaining(queue, budget)) queue.add(new Job(p.getId(), m, false));
            }
        }

        // C. 临时停用（余额类）的恢复：到期自清；未到期但本轮若探测预算有余，可提前确认恢复——
        //    这里不重复探测（等 TTL 或它作为 B 类候选再判），仅在下面 apply 成功后清。

        // 串行跑确认探测（空闲门控已在 loop 层，单请求间不再并行烧额度）
        int used = 0;
        for (Job j : queue) {
            if (used >= budget) break;
            ProbeOutcome po = probeOne(j);   // probeOne 内部多 key 试，成功即停；失败取一致分类
            used++;
            // Key 归属：成功行记真正应答的那把 Key；失败行全 Key 都试过，留 NULL 不归给任一把
            statsService.logRequest(j.model, j.provider, j.model, null, (int) po.costMs,
                    null, null, po.ok ? "OK" : "FAILED", 1, null, po.error, "reconcile",
                    null, null, null, null, po.channel());
            Set<String> set = lists.get(j.provider);
            boolean inList = set != null && j.model != null && set.contains(j.model.toLowerCase());
            Action a = decide(po.ran, po.ok, po.cls, inList, j.recoverIntent);
            apply(j, a, minFailures, evidence);
        }
        for (String[] rc : recoverByList) {
            Action a = decide(false, false, null, true, true);   // 列表命中：decide 直接给 RECOVER
            apply(new Job(rc[0], rc[1], true), a, minFailures, evidence);
        }

        flushConfigOps();

        if (!lastActions.isEmpty()) log.info("自动停用器本轮动作 {} 项（探测 {} 次）", lastActions.size(), used);
    }

    /** 判定 + 记录动作；配置类动作累积到 pendingOps 末尾一次性落盘，内存类（临时停用）即时生效 */
    private void apply(Job j, Action a, int minFailures, Map<String, long[]> evidence) {
        long[] ev = evidence.getOrDefault(j.provider + "\n" + j.model, new long[]{0, 0});
        String reason = "证据 notFound=" + ev[0] + " quota=" + ev[1] + "（阈值 " + minFailures + "）";
        switch (a) {
            case PERMANENT_DISABLE -> {
                pendingDisable.add(new String[]{j.provider, j.model});
                record(j.provider, j.model, "永久停用", "确认模型不存在", reason);
            }
            case TEMP_DISABLE -> {
                policyService.setTempDisabled(j.provider, j.model, nextMidnightMs());
                record(j.provider, j.model, "临时停用", "确认余额/额度不足", reason);
            }
            case RECOVER -> {
                if (j.recoverIntent) {
                    pendingRecover.add(new String[]{j.provider, j.model});
                    policyService.clearTempDisabled(j.provider, j.model);
                    record(j.provider, j.model, "恢复启用", "重新出现在上游列表或调用成功", reason);
                }
            }
            case NONE -> { /* 不动 */ }
        }
    }

    // ---- 配置写入合并（一轮一次 persist）----
    private final List<String[]> pendingDisable = new ArrayList<>();
    private final List<String[]> pendingRecover = new ArrayList<>();

    private void flushConfigOps() {
        if (pendingDisable.isEmpty() && pendingRecover.isEmpty()) return;
        List<String[]> dis = List.copyOf(pendingDisable);
        List<String[]> rec = List.copyOf(pendingRecover);
        pendingDisable.clear();
        pendingRecover.clear();
        try {
            configService.persist(cfg -> {
                for (String[] k : rec) {
                    GatewayConfig.RoutingOverride ro = findOverride(cfg, k[0], k[1]);
                    if (ro != null) { ro.setEnabled(null); ro.setAutoDisabled(null); }
                }
                for (String[] k : dis) {
                    GatewayConfig.RoutingOverride ro = findOrCreateOverride(cfg, k[0], k[1]);
                    ro.setEnabled(Boolean.FALSE);
                    ro.setAutoDisabled(Boolean.TRUE);
                }
            });
        } catch (Exception e) {
            log.warn("自动停用器落盘失败（{} 停/{} 复）: {}", dis.size(), rec.size(), e.getMessage());
        }
    }

    private static GatewayConfig.RoutingOverride findOverride(GatewayConfig cfg, String pid, String model) {
        for (GatewayConfig.RoutingOverride ro : cfg.getRoutingOverrides())
            if (pid.equals(ro.getProviderId()) && java.util.Objects.equals(model, ro.getModel())) return ro;
        return null;
    }

    private static GatewayConfig.RoutingOverride findOrCreateOverride(GatewayConfig cfg, String pid, String model) {
        GatewayConfig.RoutingOverride ro = findOverride(cfg, pid, model);
        if (ro != null) return ro;
        ro = new GatewayConfig.RoutingOverride();
        ro.setProviderId(pid);
        ro.setModel(model);
        cfg.getRoutingOverrides().add(ro);
        return ro;
    }

    /** 手动解除自动停用（管理页撤销按钮）：清 enabled/autoDisabled 标记，幂等。 */
    public void undoDisable(String providerId, String model) throws Exception {
        configService.persist(cfg -> {
            GatewayConfig.RoutingOverride ro = findOverride(cfg, providerId, model);
            if (ro != null && Boolean.TRUE.equals(ro.getAutoDisabled())) { ro.setEnabled(null); ro.setAutoDisabled(null); }
        });
    }

    /** 管理页：开关 + 最近一轮 + 动作 + 临时停用清单 */
    public Map<String, Object> view() {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled", d.isAutoDisableEnabled());
        out.put("intervalMinutes", d.getReconcileIntervalMinutes());
        out.put("minFailures", d.getReconcileMinFailures());
        out.put("probeBudget", d.getReconcileProbeBudget());
        out.put("lastRoundAt", lastRoundAt);
        out.put("actions", List.copyOf(lastActions));
        List<Map<String, Object>> temp = new ArrayList<>();
        for (String k : policyService.tempDisabledKeys()) {
            String[] parts = k.split("\n", 2);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("provider", parts[0]);
            m.put("model", parts.length > 1 ? parts[1] : "");
            temp.add(m);
        }
        out.put("tempDisabled", temp);
        // 本机当前"永久自动停用中"清单（配置里的 autoDisabled 标记，跨轮次持久，供界面恒显"效果已生效"）
        List<Map<String, Object>> auto = new ArrayList<>();
        for (GatewayConfig.RoutingOverride ro : configService.snapshot().config.getRoutingOverrides()) {
            if (!Boolean.TRUE.equals(ro.getAutoDisabled()) || !Boolean.FALSE.equals(ro.getEnabled())) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("provider", ro.getProviderId());
            m.put("model", ro.getModel());
            auto.add(m);
        }
        out.put("autoDisabledNow", auto);
        return out;
    }

    // ---- 探测：单模型多 key 试到成功；全失败时取"一致分类"，mixed/其它 → OTHER 让 decide 不动手 ----
    private ProbeOutcome probeOne(Job j) {
        ConfigService.Snapshot snap = configService.snapshot();
        List<GatewayConfig.Channel> chs = snap.enabledChannels(j.provider);
        GatewayConfig.Provider p = null;
        for (GatewayConfig.Provider pp : snap.config.getProviders()) if (pp.getId().equals(j.provider)) p = pp;
        if (p == null || chs.isEmpty()) return new ProbeOutcome(false, false, ErrorClass.OTHER, 0, "无可用渠道", null);

        int maxTokens = snap.config.getDefaults().getProbeMaxTokens();
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("model", j.model);
        req.put("stream", false);
        req.put("max_tokens", Math.max(1, maxTokens));
        req.put("messages", List.of(Map.of("role", "user", "content", "ping")));

        long start = System.nanoTime();
        boolean anyOk = false;
        String okChannel = null;
        Set<ErrorClass> failClasses = new LinkedHashSet<>();
        String firstErr = null;
        for (GatewayConfig.Channel ch : chs) {
            PolicyService.Candidate c = new PolicyService.Candidate(j.provider, j.model, ch.getId(), 1, null);
            ForwardEngine.ForwardResult r;
            try {
                r = forwardEngine.execute(req, c, false, System.nanoTime() + 30_000_000_000L);
            } catch (Exception e) {
                failClasses.add(ErrorClass.OTHER);
                if (firstErr == null) firstErr = "探测异常: " + e.getMessage();
                continue;
            }
            if (r.kind == ForwardEngine.ForwardResult.Kind.SUCCESS) {
                anyOk = true;
                okChannel = r.channelId != null ? r.channelId : ch.getId();
                policyService.clearTempDisabled(j.provider, j.model);
                break;
            }
            String msg = r.error != null ? r.error : (r.body != null ? r.body : ("HTTP " + r.status));
            if (firstErr == null) firstErr = msg;
            failClasses.add(UpstreamErrorClassifier.classify(r.status, msg));
        }
        long costMs = (System.nanoTime() - start) / 1_000_000;
        if (anyOk) return new ProbeOutcome(true, true, null, costMs, null, okChannel);
        ErrorClass cls = failClasses.size() == 1 ? failClasses.iterator().next() : ErrorClass.OTHER;
        return new ProbeOutcome(true, false, cls, costMs, firstErr, null);
    }

    private void record(String pid, String model, String action, String result, String detail) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("provider", pid);
        m.put("model", model);
        m.put("action", action);
        m.put("result", result);
        m.put("detail", detail);
        m.put("at", LocalTime.now().withNano(0).toString());
        lastActions.add(m);
    }

    private static boolean budgetRemaining(List<Job> queue, int budget) {
        return queue.size() < budget;
    }

    private static long asLong(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }

    /** 下一个本地零点（毫秒）——余额类临时停用的默认到期：次日额度刷新自然可再试 */
    private static long nextMidnightMs() {
        return LocalDateTime.of(LocalDate.now().plusDays(1), LocalTime.MIDNIGHT)
                .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private record Job(String provider, String model, boolean recoverIntent) {}

    /** channel：成功时=真正应答的那把 Key；失败时=null（所有 Key 都试过，归到任一把都会误导统计）。 */
    private record ProbeOutcome(boolean ran, boolean ok, ErrorClass cls, long costMs, String error, String channel) {}
}
