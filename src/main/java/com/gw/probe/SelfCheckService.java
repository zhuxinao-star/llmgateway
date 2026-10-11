package com.gw.probe;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.forward.ContextCompressor;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 压缩链路一键自检（SelfCheck）：管理页一个按钮，串行跑三类检查并出一张体检报告卡片，
 * 当场暴露"配置改坏了某条链路"这类回归——2a 事故（5s 响应头超时误伤压缩候选链 → 静默退
 * 硬截断、请求全成功、日志全绿）正是本功能要防的那类"名存实亡"。
 *
 * 三类检查（全程串行，避免体检自己把免费上游打出限流）：
 * 1. 上游探测：复用 ProbeScheduler 一轮巡检（client=probe，隔离）；
 * 2. 压缩链路干跑：ContextCompressor.selfCheckCompress() 走真实压缩候选链，固定 ~5K 样本、
 *    绕开滚动缓存（client=selfcheck，隔离）；
 * 3. 工具调用测试：每个 free 档模型发一条带 trivial 工具的极小请求，看响应里有没有 tool_calls
 *    （client=selfcheck，隔离；✗ 只展示，绝不自动改路由）。
 *
 * 隔离保证（关键）：体检请求全部带 client ∈ {probe, selfcheck}，PolicyService 的隔离集合把它们
 * 挡在 EWMA/中位数/吞吐/预算/限流学习/主熔断之外——体检绝不影响真实选举。
 * 三项都只读现有链路、不碰转发主流程；出问题最坏是"体检本身不工作"，不波及请求成功率。
 */
@Service
public class SelfCheckService {

    private static final Logger log = LoggerFactory.getLogger(SelfCheckService.class);
    private static final String SELF_CLIENT = "selfcheck";

    private final ConfigService configService;
    private final PolicyService policyService;
    private final ForwardEngine forwardEngine;
    private final ContextCompressor contextCompressor;
    private final ProbeScheduler probeScheduler;
    private final StatsService statsService;

    /** 体检串行执行器：单虚拟线程，天然串行、不常驻 */
    private final ExecutorService runner = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("gw-selfcheck-", 0).factory());
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** 最近一次体检状态与结果（管理页轮询）；volatile 整体替换，读侧拿到的是一致快照 */
    private volatile Map<String, Object> status = initial();

    public SelfCheckService(ConfigService configService, PolicyService policyService,
                            ForwardEngine forwardEngine, ContextCompressor contextCompressor,
                            ProbeScheduler probeScheduler, StatsService statsService) {
        this.configService = configService;
        this.policyService = policyService;
        this.forwardEngine = forwardEngine;
        this.contextCompressor = contextCompressor;
        this.probeScheduler = probeScheduler;
        this.statsService = statsService;
    }

    private static Map<String, Object> initial() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("state", "idle");
        m.put("note", "尚未运行体检（点\"开始体检\"串行跑：上游探测 + 压缩干跑 + 工具调用测试）");
        return m;
    }

    /** 异步启动一轮体检；已在运行则忽略（返回 false），避免并发体检互烧配额 */
    public boolean start() {
        if (!running.compareAndSet(false, true)) return false;
        long t0 = System.currentTimeMillis();
        status = Map.of("state", "running", "phase", "上游探测", "startedAt",
                LocalTime.now().withNano(0).toString());
        runner.submit(() -> {
            try {
                runAll();
                status = mergeTerminal("done", null, System.currentTimeMillis() - t0);
                log.info("体检完成，耗时 {}ms", System.currentTimeMillis() - t0);
            } catch (Exception e) {
                log.warn("体检异常: {}", e.getMessage());
                status = mergeTerminal("error", String.valueOf(e.getMessage()), System.currentTimeMillis() - t0);
            } finally {
                running.set(false);
            }
        });
        return true;
    }

    public Map<String, Object> statusView() {
        Map<String, Object> out = new LinkedHashMap<>(status);
        out.put("running", running.get());
        return out;
    }

    private Map<String, Object> mergeTerminal(String state, String error, long totalMs) {
        Map<String, Object> out = new LinkedHashMap<>(status);
        out.put("state", state);
        out.put("phase", "完成");
        out.put("totalMs", totalMs);
        out.put("finishedAt", LocalTime.now().withNano(0).toString());
        if (error != null) out.put("error", error);
        return out;
    }

    private void runAll() {
        Map<String, Object> acc = new LinkedHashMap<>();
        acc.put("state", "running");
        acc.put("startedAt", status.get("startedAt"));

        // 1) 上游探测（复用 ProbeScheduler，client=probe 已隔离）
        acc.put("phase", "上游探测（巡检全部免费模型）");
        status = acc;
        Map<String, Object> probeRep;
        try {
            probeScheduler.runRoundNow();
            probeRep = probeScheduler.view();
        } catch (Exception e) {
            probeRep = Map.of("error", String.valueOf(e.getMessage()));
        }
        acc = new LinkedHashMap<>(acc);
        acc.put("probe", summarizeProbe(probeRep));
        acc.put("phase", "压缩链路干跑");
        status = acc;

        // 2) 压缩链路干跑（client=selfcheck 隔离，绕开滚动缓存）
        Map<String, Object> compressRep;
        try {
            compressRep = contextCompressor.selfCheckCompress();
        } catch (Exception e) {
            compressRep = new LinkedHashMap<>();
            compressRep.put("ok", false);
            compressRep.put("note", "干跑异常: " + e.getMessage());
        }
        acc = new LinkedHashMap<>(acc);
        acc.put("compress", compressRep);
        acc.put("phase", "工具调用测试（逐个免费模型）");
        status = acc;

        // 3) 工具调用测试
        List<Map<String, Object>> toolRows = new ArrayList<>();
        try {
            toolRows = runToolTest();
        } catch (Exception e) {
            log.warn("工具测试异常: {}", e.getMessage());
        }
        acc = new LinkedHashMap<>(acc);
        acc.put("toolTest", toolRows);
        long okTools = toolRows.stream().filter(r -> Boolean.TRUE.equals(r.get("toolCalls"))).count();
        acc.put("toolSummary", Map.of("total", toolRows.size(), "withToolCalls", okTools));
        acc.put("phase", "生成结论");
        status = acc;

        acc = new LinkedHashMap<>(acc);
        acc.put("verdict", buildVerdict(compressRep, probeRep, toolRows));
        status = acc;
    }

    private Map<String, Object> summarizeProbe(Map<String, Object> probeRep) {
        Object results = probeRep.get("results");
        int total = 0, ok = 0;
        List<Map<String, Object>> failed = new ArrayList<>();
        if (results instanceof List<?> list) {
            for (Object o : list) {
                Map<String, Object> r = Json.asMap(o);
                if (r == null) continue;
                total++;
                if (Boolean.TRUE.equals(r.get("ok"))) ok++;
                else {
                    Map<String, Object> f = new LinkedHashMap<>();
                    f.put("provider", r.get("provider"));
                    f.put("model", r.get("model"));
                    f.put("error", r.get("error"));
                    failed.add(f);
                }
            }
        }
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("lastRoundAt", probeRep.get("lastRoundAt"));
        s.put("total", total);
        s.put("ok", ok);
        s.put("failed", failed);
        return s;
    }

    /** 工具调用测试：每个 free 档、未被探测豁免、已启用的模型发一条带 get_weather 的极小请求 */
    private List<Map<String, Object>> runToolTest() {
        List<Map<String, Object>> rows = new ArrayList<>();
        ConfigService.Snapshot snap = configService.snapshot();
        int maxTokens = Math.max(32, snap.config.getDefaults().getProbeMaxTokens() * 4);
        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            if (!p.isEnabled()) continue;
            for (String m : p.getStaticModels()) {
                if (snap.excludedFromAutoAll(p.getId(), m)) continue;
                if (snap.effectiveExcludeFromProbe(p.getId(), m)) continue;  // 配额硬顶模型不为体检花钱
                if (snap.modelDisabled(p.getId(), m)) continue;
                if (!"free".equals(snap.effectiveCostTier(p.getId(), m))) continue;  // 只测免费档（付费不为体检花钱）
                List<GatewayConfig.Channel> chs = snap.enabledChannels(p.getId());
                if (chs.isEmpty()) continue;
                PolicyService.Candidate c = new PolicyService.Candidate(p.getId(), m, chs.get(0).getId(), 1, null);
                rows.add(toolProbe(c, maxTokens));
            }
        }
        return rows;
    }

    private Map<String, Object> toolProbe(PolicyService.Candidate c, int maxTokens) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("model", c.model());
        req.put("stream", false);
        req.put("max_tokens", maxTokens);
        req.put("messages", List.of(
                Map.of("role", "user", "content", "北京今天天气怎么样？必须调用工具查询后再回答。")));
        req.put("tools", List.of(Map.of(
                "type", "function",
                "function", Map.of(
                        "name", "get_weather",
                        "description", "查询指定城市的实时天气",
                        "parameters", Map.of(
                                "type", "object",
                                "properties", Map.of("city", Map.of("type", "string", "description", "城市名")),
                                "required", List.of("city"))))));
        req.put("tool_choice", "auto");

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("provider", c.providerId());
        row.put("model", c.model());
        long t0 = System.currentTimeMillis();
        ForwardEngine.ForwardResult r;
        try {
            r = forwardEngine.execute(req, c, false, System.nanoTime() + 30_000_000_000L);
        } catch (Exception e) {
            row.put("toolCalls", false);
            row.put("error", "转发异常: " + e.getMessage());
            row.put("costMs", System.currentTimeMillis() - t0);
            statsService.logRequest(c.model(), c.providerId(), c.model(), null,
                    (int) (System.currentTimeMillis() - t0), null, null, "FAILED", 1, null,
                    "selfcheck 工具测试转发异常", SELF_CLIENT, null, null, null, null, c.channelId());
            return row;
        }
        long costMs = System.currentTimeMillis() - t0;
        row.put("costMs", costMs);
        if (r != null && r.kind == ForwardEngine.ForwardResult.Kind.SUCCESS && r.parsedBody != null) {
            List<?> choices = Json.asList(r.parsedBody.get("choices"));
            Map<String, Object> first = choices == null || choices.isEmpty() ? null : Json.asMap(choices.get(0));
            Map<String, Object> msg = first == null ? null : Json.asMap(first.get("message"));
            boolean hasCalls = msg != null && msg.get("tool_calls") instanceof List<?> l && !l.isEmpty();
            Object finishReason = first == null ? null : first.get("finish_reason");
            Map<String, Object> usage = Json.asMap(r.parsedBody.get("usage"));
            Long tIn = usage == null ? null : Json.longVal(usage.get("prompt_tokens"));
            Long tOut = usage == null ? null : Json.longVal(usage.get("completion_tokens"));
            row.put("toolCalls", hasCalls);
            row.put("finishReason", finishReason);
            // #33：体检工具结果喂给 toolWeak 学习计数器（trivial 工具都调不出=强证据，但单条不足触发阈值，只加速）
            policyService.recordToolSample(c.providerId(), c.model(), true, hasCalls);
            statsService.logRequest(c.model(), c.providerId(), c.model(), null, (int) costMs, tIn, tOut,
                    "OK", 1, null, hasCalls ? null : "no tool_calls", SELF_CLIENT,
                    null, null, 200, null, c.channelId());
        } else {
            row.put("toolCalls", false);
            String err = r == null ? "无响应" : (r.error != null ? r.error : "HTTP " + r.status);
            row.put("error", err);
            statsService.logRequest(c.model(), c.providerId(), c.model(), null, (int) costMs, null, null,
                    "FAILED", 1, null, err, SELF_CLIENT, null, null,
                    r != null && r.status > 0 ? r.status : null, null, c.channelId());
        }
        return row;
    }

    /** 综合结论：压缩链路是本网关核心增值能力——它红则整张报告红（复刻 2a 事故的发现路径） */
    private Map<String, Object> buildVerdict(Map<String, Object> compressRep, Map<String, Object> probeRep,
                                             List<Map<String, Object>> toolRows) {
        boolean compressOk = Boolean.TRUE.equals(compressRep.get("ok"));
        boolean compressSkipped = Boolean.TRUE.equals(compressRep.get("skipped"));
        Map<String, Object> ps = summarizeProbe(probeRep);
        int probeTotal = (int) ps.getOrDefault("total", 0);
        int probeOk = (int) ps.getOrDefault("ok", 0);
        long toolOk = toolRows.stream().filter(x -> Boolean.TRUE.equals(x.get("toolCalls"))).count();

        String level;
        String text;
        if (!compressOk && !compressSkipped) {
            level = "error";
            text = "压缩链路不可用：真实大上下文请求会静默退化到硬截断（无摘要）——正是 2a 类事故，需立即排查。";
        } else if (compressSkipped) {
            level = "warn";
            text = "压缩功能未启用（未配置 contextCompressModel），干跑跳过。";
        } else if (probeTotal > 0 && probeOk == 0) {
            level = "warn";
            text = "压缩链路正常，但本轮巡检无任何免费模型健康，请检查上游网络/额度。";
        } else {
            level = "ok";
            text = String.format("压缩链路可用；巡检 %d/%d 模型健康；工具调用 %d/%d 模型能发起工具。",
                    probeOk, probeTotal, toolOk, toolRows.size());
        }
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("level", level);
        v.put("text", text);
        return v;
    }
}
