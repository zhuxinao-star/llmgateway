package com.gw.probe;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.forward.ForwardEngine;
import com.gw.policy.PolicyService;
import com.gw.provider.adaptor.Json;
import com.gw.stats.StatsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PostConstruct;

/**
 * 定时探测调度器：空闲期主动巡检全部候选模型，把"选举"从被动变主动。
 *
 * 用户痛点（2026-09-14）：网关平时只靠真实流量的失败/超时被动学习，
 * 长期空闲的模型（如 agnes/openrouter）没有新样本——EWMA 陈旧、熔断了也没人探测恢复，
 * 一旦前排候选挂掉才临时"抽盲盒"。本调度器周期性做三件事：
 *
 * 1. 健康探测：每个 provider×model 发一条 max_tokens 极小的请求（默认 8 token），
 *    结果写"模型巡检"卡片与请求日志（client='probe'）供人查看；
 *    不再回流选举——探测隔离（PolicyService 内按 client=probe 判定）：
 *    失败不进主熔断、TTFT 不进 EWMA、token 不进预算、不进限流学习窗口。
 *    理由：ping 样本代表不了真实大请求的速度/健康，空闲期 probe 占请求大头，
 *    一次网络抖动就把可用模型打出候选池是净伤害。
 * 2. 熔断提前恢复：OPEN 状态的条目冷却过半即由探测请求验证——探测成功走 recordSuccess
 *    的半开恢复路径，比干等冷却结束更早回归候选池（唯一的熔断状态写入点）。
 * 3. 巡检留痕：结果写 request_log（client='probe'）——请求日志页可查历史，用量页
 *    按工具统计也能看到 probe 桶（真实请求统计建议看日志页，probe 桶只反映巡检开销）。
 *
 * 省钱设计：探测只对 free 档模型进行（付费模型绝不为探测花钱）；max_tokens=8 一次
 * 最多烧十几个 token；间隔默认 30 分钟一巡。excludeFromAutoAll 的图像模型跳过
 * （聊天探测必失败，没有意义）。
 */
@Service
public class ProbeScheduler {

    private static final Logger log = LoggerFactory.getLogger(ProbeScheduler.class);

    /** #13 空闲门控的最大延后时长：有流量时探测让路，但累计延后超此时长强制执行一轮
     *  （边界 1：否则持续流量期半开熔断恢复会饿死，probe 是 tryEnterHalfOpen 的唯一触发点） */
    private static final long MAX_DEFER_MS = 10 * 60_000L;

    private final ConfigService configService;
    private final PolicyService policyService;
    private final ForwardEngine forwardEngine;
    private final StatsService statsService;

    /** 最近一轮巡检结果（管理页展示）：key = providerId \n model */
    private final ConcurrentHashMap<String, Map<String, Object>> lastRound = new ConcurrentHashMap<>();
    private volatile String lastRoundAt = null;

    public ProbeScheduler(ConfigService configService, PolicyService policyService,
                          ForwardEngine forwardEngine, StatsService statsService) {
        this.configService = configService;
        this.policyService = policyService;
        this.forwardEngine = forwardEngine;
        this.statsService = statsService;
    }

    @PostConstruct
    public void start() {
        Thread.ofVirtual().name("gw-probe-scheduler").start(this::loop);
    }

    private void loop() {
        try { Thread.sleep(90_000); } catch (InterruptedException e) { return; }  // 启动等 90s：配置/目录就绪后再谈巡检
        long lastAt = 0;
        long deferSince = 0;   // #13 本轮因"有在途流量"首次被延后的时刻（0=未延后）
        while (true) {
            try {
                GatewayConfig cfg;
                try {
                    cfg = configService.snapshot().config; // 配置未就绪会抛 IllegalStateException
                } catch (IllegalStateException e) {
                    Thread.sleep(30_000);
                    continue;
                }
                int intervalMin = cfg.getDefaults().getProbeIntervalMinutes();
                if (intervalMin <= 0) {   // 0=关闭
                    Thread.sleep(60_000);
                    continue;
                }
                long now = System.currentTimeMillis();
                if (now - lastAt >= intervalMin * 60_000L) {
                    // #13 空闲门控：探测请求与真实请求抢同一份免费 RPM（日志实证 probe 曾占 ~72% 日志量）。
                    // 有在途流量就让路延后；但延后累计超 MAX_DEFER_MS 强制执行一轮（边界 1，防半开恢复饿死）。
                    // 注：probe 自身走 forwardEngine 不经 acquireInflight，totalInflight 只反映真实流量。
                    int inflight = policyService.totalInflight();
                    boolean forced = deferSince != 0 && now - deferSince >= MAX_DEFER_MS;
                    if (inflight > 0 && !forced) {
                        if (deferSince == 0) deferSince = now;
                        log.debug("探测延后本轮：在途 {} 条（已延后 {}s / 上限 {}s）",
                                inflight, (now - deferSince) / 1000, MAX_DEFER_MS / 1000);
                    } else {
                        if (forced) log.info("探测强制执行一轮：延后累计超 {} 分钟（当前在途 {} 条）",
                                MAX_DEFER_MS / 60000, inflight);
                        lastAt = now;
                        deferSince = 0;
                        runRound();
                    }
                }
                // 60s 轮询而非 10min 定睡：定睡会把 30min 配置漂成 30~40min，且支持更短间隔
                Thread.sleep(60_000L);
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                log.warn("探测巡检异常: {}", e.getMessage());
                try { Thread.sleep(30_000); } catch (InterruptedException ie) { return; }
            }
        }
    }

    /** 手动触发一轮巡检（管理页按钮）：阻塞执行完返回 */
    public void runRoundNow() { runRound(); }

    /** 一轮巡检：全部启用的 free 档模型逐个探测（串行，避免并发打出限流） */
    private void runRound() {
        ConfigService.Snapshot snap = configService.snapshot();
        int maxTokens = snap.config.getDefaults().getProbeMaxTokens();
        lastRound.clear();
        lastRoundAt = LocalTime.now().withNano(0).toString();
        int ok = 0, fail = 0;

        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            if (!p.isEnabled()) continue;
            for (String m : p.getStaticModels()) {
                if (snap.excludedFromAutoAll(p.getId(), m)) continue;       // 图像等专用模型：聊天探测无意义
                if (snap.effectiveExcludeFromProbe(p.getId(), m)) continue; // 配额硬顶模型：探测会烧掉真实日额度（管理页可勾豁免）
                if (snap.modelDisabled(p.getId(), m)) continue;             // 已停用的模型不探测（省配额、不产噪音日志）
                if (!"free".equals(snap.effectiveCostTier(p.getId(), m))) continue;  // 付费模型不为探测花钱
                List<GatewayConfig.Channel> chs = snap.enabledChannels(p.getId());
                if (chs.isEmpty()) continue;
                // 模型健康 = 任一 key(账号) 探测成功即可。逐把 key 试：
                // 某账号额度耗尽/限流不代表整家不通，换下一把 key 仍有额度即算健康。
                Map<String, Object> r = null;
                for (GatewayConfig.Channel ch : chs) {
                    PolicyService.Candidate c = new PolicyService.Candidate(p.getId(), m, ch.getId(), 1, null);
                    r = probe(c, maxTokens, snap);
                    if (Boolean.TRUE.equals(r.get("ok"))) break;   // 任一 key 成功即健康，不再多烧探测
                }
                lastRound.put(p.getId() + "\n" + m, r == null ? Map.of("ok", false, "error", "无渠道") : r);
                if (r != null && Boolean.TRUE.equals(r.get("ok"))) ok++; else fail++;
            }
        }
        log.info("探测巡检完成: {} 个模型, 成功 {} / 失败 {}", ok + fail, ok, fail);
    }

    /** 探测单个候选：小请求直发，结果记账到熔断/EWMA + request_log */
    private Map<String, Object> probe(PolicyService.Candidate c, int maxTokens, ConfigService.Snapshot snap) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("model", c.model());
        req.put("stream", false);
        req.put("max_tokens", Math.max(1, maxTokens));
        req.put("messages", List.of(Map.of("role", "user", "content", "ping")));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("provider", c.providerId());
        out.put("model", c.model());
        long start = System.nanoTime();
        ForwardEngine.ForwardResult r;
        try {
            r = forwardEngine.execute(req, c, false, System.nanoTime() + 30_000_000_000L);
        } catch (Exception e) {
            r = null;
            out.put("ok", false);
            out.put("error", "探测异常: " + e.getMessage());
        }
        long costMs = (System.nanoTime() - start) / 1_000_000;

        if (r != null) {
            if (r.kind == ForwardEngine.ForwardResult.Kind.SUCCESS) {
                Map<String, Object> usage = Json.asMap(r.parsedBody.get("usage"));
                Long tIn = usage == null ? null : Json.longVal(usage.get("prompt_tokens"));
                Long tOut = usage == null ? null : Json.longVal(usage.get("completion_tokens"));
                // 熔断恢复：若该条目在 OPEN 冷却且已过半，探测成功即走 recordSuccess 的半开恢复分支
                policyService.tryEnterHalfOpen(c.providerId(), c.model());
                policyService.recordSuccess(c.providerId(), c.model(), c.channelId(), r.ttftMs / 1_000_000,
                        tIn, tOut, false, "probe", costMs);
                // #13 探测行补记 ttft/total：原为 null → 探测延迟历史不可回溯，无法区分"慢"与"挂"
                statsService.logRequest(c.model(), c.providerId(), c.model(),
                        (int) (r.ttftMs / 1_000_000), (int) costMs, tIn, tOut, "OK", 1, null, null, "probe",
                        null, null, null, null, c.channelId());
                out.put("ok", true);
                out.put("ttftMs", r.ttftMs / 1_000_000);
                out.put("costMs", costMs);
                out.put("tokensIn", tIn);
                out.put("tokensOut", tOut);
            } else {
                String err = r.error != null ? r.error : ("HTTP " + r.status);
                policyService.recordFailure(c.providerId(), c.model(), c.channelId(), r.status, err, "probe");
                statsService.logRequest(c.model(), c.providerId(), c.model(), null, (int) costMs,
                        null, null, "FAILED", 1, null, err, "probe",
                        null, null, r.status > 0 ? r.status : null, null, c.channelId());
                out.put("ok", false);
                out.put("error", err);
                out.put("costMs", costMs);
            }
        }
        return out;
    }

    /** 管理页：最近一轮巡检结果（系统页"模型巡检"卡片） */
    public Map<String, Object> view() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("lastRoundAt", lastRoundAt);
        out.put("intervalMinutes", configService.snapshot().config.getDefaults().getProbeIntervalMinutes());
        out.put("results", List.copyOf(lastRound.values()));
        return out;
    }
}
