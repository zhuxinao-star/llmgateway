package com.gw.gateway;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.forward.ContextCompressor;
import com.gw.forward.ForwardEngine;
import com.gw.forward.HedgedExecutor;
import com.gw.policy.PolicyService;
import com.gw.policy.RequestCategoryAnalyzer;
import com.gw.provider.adaptor.AnthropicConverter;
import com.gw.provider.adaptor.Json;
import com.gw.router.RouterService;
import com.gw.stats.StatsService;
import com.gw.util.ClientDetector;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Anthropic 原生协议接入层（Claude Code 直连用）。
 *
 * POST /v1/messages  → 内部转 OpenAI 格式走既有路由 → 响应/流式转回 Anthropic 格式
 * GET /v1/models     → Anthropic 格式的模型清单（Claude Code 启动时拉）
 *
 * 鉴权：Authorization: Bearer <gateway-key> 或 x-api-key: <gateway-key>
 */
@RestController
public class AnthropicController {

    private static final Logger log = LoggerFactory.getLogger(AnthropicController.class);
    /** 模型不存在时的回落组（与 GatewayController 一致） */
    private static final String DEFAULT_FALLBACK_MODEL = "free-fast";

    private final ConfigService configService;
    private final RouterService routerService;
    private final PolicyService policyService;
    private final ForwardEngine forwardEngine;
    private final HedgedExecutor hedgedExecutor;
    private final StatsService statsService;
    private final GatewayController gatewayController;  // 复用鉴权
    private final ContextCompressor contextCompressor;
    private final ClientQueueService clientQueueService;
    private final com.gw.policy.RankTraceStore rankTraceStore;

    public AnthropicController(ConfigService configService, RouterService routerService,
                                PolicyService policyService, ForwardEngine forwardEngine,
                                HedgedExecutor hedgedExecutor, StatsService statsService,
                                GatewayController gatewayController, ContextCompressor contextCompressor,
                                ClientQueueService clientQueueService,
                                com.gw.policy.RankTraceStore rankTraceStore) {
        this.configService = configService;
        this.routerService = routerService;
        this.policyService = policyService;
        this.forwardEngine = forwardEngine;
        this.hedgedExecutor = hedgedExecutor;
        this.statsService = statsService;
        this.gatewayController = gatewayController;
        this.contextCompressor = contextCompressor;
        this.clientQueueService = clientQueueService;
        this.rankTraceStore = rankTraceStore;
    }

    /** Anthropic 风格错误响应 */
    private void anthropicError(HttpServletResponse response, int status, String type, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        writeUtf8(response, Json.write(Json.obj("type", "error",
                "error", Json.obj("type", type, "message", message))));
    }

    // /v1/models 由 GatewayController 提供（OpenAI 格式，Claude Code 可解析）。
    // Anthropic 客户端若请求 GET /v1/models 带 x-api-key，GatewayController 同样鉴权放行。

    // ================= POST /v1/messages/count_tokens =================

    /** Claude Code 在每次请求前调它预估上下文长度（决定压缩/报错）。没有这个端点会 404，
     *  部分版本直接中止会话——用与路由同一套估算器给出近似值即可。 */
    @PostMapping("/v1/messages/count_tokens")
    public void countTokens(@RequestHeader(value = "Authorization", required = false) String auth,
                            @RequestHeader(value = "x-api-key", required = false) String apiKeyHeader,
                            @RequestHeader(value = "User-Agent", required = false) String userAgent,
                            @RequestBody String body,
                            HttpServletResponse response) throws IOException {
        String client = ClientDetector.detect(userAgent);
        com.gw.stats.RequestTrace.start(response);
        if (authError(auth, apiKeyHeader)) {
            logRejected("-", client, null, "unauthorized 401");
            anthropicError(response, 401, "authentication_error", "invalid x-api-key");
            return;
        }
        Map<String, Object> req;
        try {
            req = Json.parseObject(body);
        } catch (Exception e) {
            logRejected("-", client, null, "bad_request: count_tokens JSON");
            anthropicError(response, 400, "invalid_request_error", "请求体不是合法 JSON: " + e.getMessage());
            return;
        }
        String model = Json.str(req.get("model"));
        Map<String, Object> openAiReq = AnthropicConverter.anthropicRequestToOpenai(req, model == null ? "-" : model);
        int tokens = RequestCategoryAnalyzer.estimateInputTokens(openAiReq);
        response.setContentType("application/json");
        writeUtf8(response, Json.write(Json.obj("input_tokens", Math.max(1, tokens))));
    }

    // ================= POST /v1/messages =================

    @PostMapping("/v1/messages")
    public void messages(@RequestHeader(value = "Authorization", required = false) String auth,
                         @RequestHeader(value = "x-api-key", required = false) String apiKeyHeader,
                         @RequestHeader(value = "anthropic-version", required = false) String version,
                         @RequestHeader(value = "X-Gateway-Profile", required = false) String profile,
                         @RequestHeader(value = "User-Agent", required = false) String userAgent,
                         @RequestBody String body,
                         HttpServletResponse response) throws IOException {
        String client = ClientDetector.detect(userAgent);
        com.gw.stats.RequestTrace.start(response);   // 早于鉴权：REJECTED 行也带 trace
        if (authError(auth, apiKeyHeader)) {
            logRejected("-", client, null, "unauthorized 401");
            anthropicError(response, 401, "authentication_error", "invalid x-api-key");
            return;
        }
        boolean critical = "critical".equalsIgnoreCase(profile);

        Map<String, Object> req;
        try {
            req = Json.parseObject(body);
        } catch (Exception e) {
            logRejected("-", client, profile, "bad_request: 请求体不是合法 JSON");
            anthropicError(response, 400, "invalid_request_error", "请求体不是合法 JSON: " + e.getMessage());
            return;
        }
        String model = Json.str(req.get("model"));
        if (model == null || model.isBlank()) {
            logRejected("-", client, profile, "bad_request: model 字段缺失");
            anthropicError(response, 400, "invalid_request_error", "model 字段缺失");
            return;
        }
        boolean stream = Json.boolVal(req.get("stream"), false);

        // ---- Anthropic 请求 → OpenAI 格式，走既有路由 ----
        Map<String, Object> openAiReq = AnthropicConverter.anthropicRequestToOpenai(req, model);
        // 注：model 字段保留原始名（供路由解析），转发层会替换为各候选的真实模型名
        openAiReq.put("model", model);

        List<PolicyService.Candidate> candidates = routerService.resolve(model);
        // 模型不存在时回落默认组 free-fast（Claude Code 配置一次即可）
        if (candidates.isEmpty() && !model.equals(DEFAULT_FALLBACK_MODEL)) {
            List<PolicyService.Candidate> fb = routerService.resolve(DEFAULT_FALLBACK_MODEL);
            if (!fb.isEmpty()) {
                log.warn("[anthropic] 模型 {} 不存在，回落默认组 {}", model, DEFAULT_FALLBACK_MODEL);
                candidates = fb;
            }
        }
        if (candidates.isEmpty()) {
            logRejected(model, client, profile, "model_not_found 404");
            anthropicError(response, 404, "invalid_request_error", "model: 模型不存在: " + model);
            return;
        }
        String strategy = configService.snapshot().config.getStrategy();
        // ---- 上下文自动压缩（同 OpenAI 端点；Anthropic system 是顶层字段已并入 messages）----
        ContextCompressor.Compressed cmp = contextCompressor.maybeCompress(openAiReq, client);
        if (cmp.compressed()) response.setHeader("X-Gateway-Context-Compressed", "true");
        // 语义路由：分析转换前 Anthropic 请求（user 内容已在 openAiReq.messages 里）
        String requestCategory = RequestCategoryAnalyzer.analyze(openAiReq);
        int estIn = cmp.compressed() ? cmp.tokensAfter() : RequestCategoryAnalyzer.estimateInputTokens(openAiReq);
        PolicyService.RankTrace rt = rankTraceStore.begin(com.gw.stats.RequestTrace.get(), model, client, "/v1/messages");
        List<PolicyService.Scored> ranked = policyService.rank(candidates, strategy, critical, requestCategory,
                estIn, RequestCategoryAnalyzer.requiresTools(openAiReq), client, rt);
        if (rt != null) rankTraceStore.commit(rt);
        if (ranked.isEmpty()) {
            logRejected(model, client, profile, "no_candidate 529" + GatewayController.tierHint(strategy));
            anthropicError(response, 529, "overloaded_error",
                    "所有候选渠道均不可用" + GatewayController.tierHint(strategy) + ": " + model);
            return;
        }

        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        long deadline = System.nanoTime() + (long) d.getTotalBudgetMs() * 1_000_000;

        // ---- 下游 SSE 保活（同 OpenAI 端点）：仅流式且 interval>0 时创建，**延迟到 streamToClient
        //  真正开始吐流时才 commit(200)+start**（见 Fix A）。开局前的硬失败仍走真实 HTTP 429/529，
        //  好让工具按退避重试——提前 commit 会把可重试错误降级成"200+流内 error"，Claude Code 对流内
        //  错误不重试，正是"整轮不回复"的根因。非流式或 interval<=0 时 ka=null。 ----
        SseKeepAlive ka = (stream && d.getSseKeepAliveIntervalMs() > 0)
                ? new SseKeepAlive(response, d.getSseKeepAliveIntervalMs()) : null;

        // ---- 同源并发队列（同 OpenAI 端点）：同一 client 并发>上限时排队，避免免费上游被打爆 ----
        ClientQueueService.QueueToken q = clientQueueService.acquire(client);
        try {
            ForwardEngine.ForwardResult lastFail = null;
            long requestStart = System.nanoTime();
            int attemptNo = 0;
            // ---- Fix C（同 OpenAI 端点）：totalBudget 内有限轮耐心 failover。首轮全败且可重试时，退避后
            //  按 pass1 学到的最新冷却/熔断重排名再来一轮，透明扛住免费上游瞬时限流；非重试类立即放弃。
            //  封顶 2 轮，绝不盲目狂刷加剧限流。 ----
            List<PolicyService.Scored> passRanked = ranked;
            final int maxPasses = 2;
            for (int pass = 1; pass <= maxPasses; pass++) {
                int attemptsThisPass = Math.min(Math.max(1, passRanked.size()),
                        Math.min(Math.max(d.getMaxAttempts(), 1), 6));
                for (int i = 0; i < attemptsThisPass && System.nanoTime() < deadline; i++) {
                    attemptNo++;
                    PolicyService.Candidate c = passRanked.get(i).candidate;

                    policyService.tryEnterHalfOpen(c.providerId(), c.model());
                    policyService.acquireInflight(c.providerId(), c.model());
                    policyService.recordChannelUsed(c.providerId(), c.channelId());
                    ForwardEngine.ForwardResult r;
                    // #11 流式排空占并发：acquireInflight 原本只包住 execute（拿到首字即 release），
                    // 之后 streamToClient 排空最长 120s 却不占额度 → 免费上游的 concurrency limit 被击穿。
                    // STREAM 时把该候选的并发位一直持有到排空结束（与 OpenAI 端点同处理）。
                    boolean holdForStream = false;
                    try {
                        // 输出 token 预算学习：按候选钳制 max_tokens（同 OpenAI 端点）
                        Map<String, Object> forwardReq = new LinkedHashMap<>(openAiReq);
                        int safeMax = policyService.safeMaxOutputTokens(c.providerId(), c.model());
                        if (safeMax > 0) {
                            Object mt = forwardReq.get("max_tokens");
                            int reqMt = mt == null ? 0 : Json.longVal(mt).intValue();
                            if (reqMt == 0 || reqMt > safeMax) forwardReq.put("max_tokens", safeMax);
                        }
                        // 对冲执行：主选慢时自动竞速备选（仅流式；非流式退化为单发）
                        r = hedgedExecutor.execute(forwardReq, passRanked, i, stream, deadline);
                        holdForStream = r.kind == ForwardEngine.ForwardResult.Kind.STREAM;
                    } finally {
                        if (!holdForStream) policyService.releaseInflight(c.providerId(), c.model());
                    }

                    switch (r.kind) {
                        case FAILED -> {
                            policyService.recordFailure(c.providerId(), c.model(), c.channelId(), r.status, r.error, client);
                            // Retry-After 精确冷却（同 OpenAI 端点）
                            if ((r.status == 429 || r.status == 503) && r.retryAfterSeconds > 0) {
                                policyService.recordRateLimitWithRetryAfter(c.providerId(), c.channelId(), r.retryAfterSeconds);
                            }
                            statsService.logRequest(model, c.providerId(), c.model(), null, null,
                                    null, null, "FAILED", attemptNo, profile, r.error, client, cmp.compressTag(),
                                    null, r.status, null, c.channelId());
                            log.warn("[anthropic] 转发失败 attempt={} pass={} client={} provider={}/{} status={} err={}",
                                    attemptNo, pass, client, c.providerId(), c.model(), r.status, r.error);
                            lastFail = r;
                        }
                        case SUCCESS -> {
                            // OpenAI 响应 → Anthropic 响应
                            Map<String, Object> usage = Json.asMap(r.parsedBody.get("usage"));
                            judgeOutputQuality(c, openAiReq, r.parsedBody, client, cmp.compressTag());   // 输出质量自反馈（同 OpenAI 端点）
                            Map<String, Object> anthropicResp = AnthropicConverter.openaiResponseToAnthropic(r.parsedBody, model);
                            response.setStatus(200);
                            response.setContentType("application/json");
                            response.setHeader("X-Gateway-Channel", c.providerId() + "/" + c.channelId());
                            response.setHeader("X-Gateway-Attempt", String.valueOf(attemptNo));
                            String delivered = "OK";
                            try {
                                writeUtf8(response, Json.write(anthropicResp));
                            } catch (IOException e) {
                                delivered = "CLIENT_ABORTED";   // 上游产出正常但工具端已断开
                                log.warn("[anthropic] 客户端提前断开(非流式) {}/{}: {}", c.providerId(), c.model(), e.getMessage());
                            }
                            recordSuccess(c, model, r.ttftMs / 1_000_000, usage, attemptNo, profile,
                                    (System.nanoTime() - requestStart) / 1_000_000, client, cmp.compressTag(), delivered);
                            return;
                        }
                        case STREAM -> {
                            // 胜者可能是对冲备选：以 ForwardResult 内身份为准
                            PolicyService.Candidate winner = winnerOf(r, c);
                            // #11 并发位一直占到流排空结束（execute 已保留 c 的额度未释放，异常也要还）
                            try {
                                streamToClient(model, winner, r, attemptNo, profile, client, openAiReq, response, cmp.compressTag(), ka);
                            } finally {
                                policyService.releaseInflight(c.providerId(), c.model());
                            }
                            return;
                        }
                    }
                }
                // ---- 一轮（全候选）结束仍未成功：仅当最后失败可重试时才再来一轮（同 OpenAI 端点） ----
                if (pass >= maxPasses) break;
                long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
                long backoffMs = GatewayController.patientBackoffMs(lastFail);
                if (backoffMs <= 0 || backoffMs >= remainingMs) break;   // 非重试类/无退避价值/预算不够=交客户端退避
                log.info("[anthropic] 候选首轮全败(可重试)，网关退避 {}ms 后按最新健康度重扫一轮 model={} client={}",
                        backoffMs, model, client);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                List<PolicyService.Scored> reRanked = policyService.rank(candidates, strategy, critical,
                        requestCategory, estIn, RequestCategoryAnalyzer.requiresTools(openAiReq), client);
                if (reRanked.isEmpty()) break;
                passRanked = reRanked;
            }
            // ---- 候选穷尽：透传最后上游错误（真实 HTTP 状态码，供工具退避重试）。
            //  走到这里说明没有任何候选返回 STREAM，ka 尚未 commit，状态码/响应头仍可写。 ----
            if (lastFail != null && lastFail.status > 0) {
                response.setStatus(lastFail.status);
                response.setContentType("application/json");
                response.setHeader("X-Gateway-Error", "all_attempts_failed");
                writeUtf8(response, lastFail.body == null ? "" : lastFail.body);
                return;
            }
            anthropicError(response, 504, "api_error", "全部候选尝试失败: " + model);
        } finally {
            if (ka != null) ka.stop();
            clientQueueService.release(q);
            com.gw.stats.RequestTrace.clear();
        }
    }

    // ================= SSE：OpenAI chunk 流 → Anthropic 事件流 =================

    private void streamToClient(String model, PolicyService.Candidate c,
                                 ForwardEngine.ForwardResult r, int attemptNo,
                                 String profile, String client, Map<String, Object> req,
                                 HttpServletResponse response, String compress, SseKeepAlive ka) throws IOException {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        long startMs = System.currentTimeMillis();
        // 延迟提交：此刻候选已真正开始吐流（execute 返回 STREAM = TTFT 窗口内拿到首字），
        // 才 commit 200 SSE 头 + 拉起 ticker，覆盖后续 token 间空档。此前任何硬失败仍回真实状态码。
        if (ka != null) {
            ka.commit();
            ka.start();
        } else {
            response.setStatus(200);
            response.setContentType("text/event-stream");
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());
            response.setHeader("X-Accel-Buffering", "no");
            response.setHeader("Cache-Control", "no-cache");
            response.setHeader("X-Gateway-Channel", c.providerId() + "/" + c.channelId());
            response.setHeader("X-Gateway-Attempt", String.valueOf(attemptNo));
        }

        String firstTokenAt = null;
        long lastRealAt = 0;   // 上一次"真内容行"时间；0=还没有（keep-alive 行不算）
        Map<String, Object> usage = null;
        BufferedReader reader = r.streamReader;
        AnthropicConverter.StreamState st = new AnthropicConverter.StreamState();
        java.io.OutputStream out = ka != null ? ka.guarded() : response.getOutputStream();
        // 流式质量信号收集（收尾统一判定软失败：截断/空回复）
        String streamFinish = null;
        boolean streamSawContent = false, streamSawToolUse = false;
        long streamContentLen = 0;   // 累计内容字符数（早停嫌疑判定）
        try (reader; out) {
            String pending = r.firstLine;   // execute 内已读出的首行（对冲竞速胜出时带出）
            while (true) {
                String line;
                if (pending != null) { line = pending; pending = null; }
                else {
                    // 窗口双档：首字前严格（快速发现死流）；已出 token 后放宽（长思考模型事件间隔可达 60~100s）
                    long win = lastRealAt == 0 ? d.getStreamHeartbeatTimeoutMs() : d.getFirstTokenHeartbeatTimeoutMs();
                    String next = pollLine(reader, win);
                    if (next == HEARTBEAT_TIMEOUT) {
                        long now = System.currentTimeMillis();
                        log.warn("[anthropic] 流中心跳超时，掐断 {}/{}", c.providerId(), c.model());                        statsService.logRequest(model, c.providerId(), c.model(), (int) (r.ttftMs / 1_000_000),
                                (int) (now - startMs), null, null, "STREAM_TIMEOUT", attemptNo, profile, "heartbeat timeout", client,
                                compress, null, null, null, c.channelId());
                        policyService.recordFailure(c.providerId(), c.model(), c.channelId(), 0, "stream heartbeat timeout", client);
                        return;
                    }
                    if (next == null) break;  // 上游 EOF
                    line = next;
                }
                long now = System.currentTimeMillis();
                // SSE 语义：注释行（: 开头）与空行永远不是内容（keep-alive），不刷新窗口
                boolean isKeepAliveLine = line.isBlank() || line.startsWith(":");
                List<String> events = isKeepAliveLine
                        ? List.of() : AnthropicConverter.openaiChunkToAnthropicEvents(line, st, model);
                if (events.isEmpty()) {
                    // keep-alive 注释行/空行不算真内容：不刷新窗口；真内容窗口耗尽则掐断
                    // （pollLine 只在"完全无数据"时超时，不断吐注释行的死流靠这里发现）
                    long win = lastRealAt == 0 ? d.getStreamHeartbeatTimeoutMs() : d.getFirstTokenHeartbeatTimeoutMs();
                    if (now - (lastRealAt == 0 ? startMs : lastRealAt) > win) {
                        log.warn("[anthropic] 流内长时间无真内容（keep-alive 行续命 {}ms），掐断 {}/{}",
                                now - (lastRealAt == 0 ? startMs : lastRealAt), c.providerId(), c.model());
                        statsService.logRequest(model, c.providerId(), c.model(), (int) (r.ttftMs / 1_000_000),
                                (int) (now - startMs), null, null, "STREAM_TIMEOUT", attemptNo, profile, "no real content window exceeded", client,
                                compress, null, null, null, c.channelId());
                        policyService.recordFailure(c.providerId(), c.model(), c.channelId(), 0, "stream no real content timeout", client);
                        return;
                    }
                    continue;
                }
                lastRealAt = now;
                // 只记首条真内容行（对齐 OpenAI 端口径）。此前无条件覆盖使 TTFT≈整段流时长，
                // Claude Code 全部流量的速度样本被系统性高估，污染 EWMA/中位数/分摊门槛。
                if (firstTokenAt == null) firstTokenAt = String.valueOf(now - startMs);
                {
                    // 接住 usage（最后一个含 usage 的 chunk）+ 流式质量信号
                    if (line.startsWith("data:")) {
                        String p = line.substring(5).strip();
                        if (!"[DONE]".equals(p)) {
                            try {
                                Map<String, Object> ev = Json.parseObject(p);
                                Map<String, Object> u = Json.asMap(ev.get("usage"));
                                if (u != null && u.get("prompt_tokens") != null) usage = u;
                                List<Object> chs = Json.asList(ev.get("choices"));
                                if (chs != null && !chs.isEmpty()) {
                                    Map<String, Object> ch = Json.asMap(chs.get(0));
                                    String frs = Json.str(ch.get("finish_reason"));
                                    if (frs != null && !frs.isBlank()) streamFinish = frs;   // 空串占位不算终止（同转换器）
                                    Map<String, Object> delta = Json.asMap(ch.get("delta"));
                                    if (delta != null) {
                                        if (delta.get("content") instanceof String t) {
                                            streamContentLen += t.length();
                                            if (!t.isBlank()) streamSawContent = true;
                                        }
                                        if (delta.get("tool_calls") != null) streamSawToolUse = true;
                                    }
                                }
                            } catch (Exception ignore) {
                            }
                        }
                    }
                    for (String e : events) {
                        // SSE 以空行（\n\n）为事件分隔符：Anthropic 官方 SDK 严格 parse，
                        // 只给单 \n 时事件会一直攒着不下发（表现为"有响应但没内容"）
                        String framed = e.endsWith("\n\n") ? e : e.endsWith("\n") ? e + "\n" : e + "\n\n";
                        out.write(framed.getBytes(StandardCharsets.UTF_8));
                    }
                    out.flush();
                }
                // [DONE]（已转换为 message_stop 事件）= 上游正常终止。但 agnes 等把末尾 usage chunk
                // 放在 finish_reason 之后：若在此直接 break 就丢用量（tokens 记 0+0）。改为有界补采——
                // 短超时再读几行、只为把 usage 累加进 st（事件丢弃不再写客户端），见 [DONE]/EOF/超短窗
                // 无数据即退，绝不死等（否则发完不关连接的上游会把正常结束误判成 STREAM_TIMEOUT）。
                if (st.finished) {
                    for (int g = 0; g < 8; g++) {
                        String nx = pollLine(reader, 1500);
                        if (nx == null || nx == HEARTBEAT_TIMEOUT) break;
                        String s = nx.strip();
                        if (s.startsWith("data:") && "[DONE]".equals(s.substring(5).strip())) break;
                        AnthropicConverter.openaiChunkToAnthropicEvents(nx, st, model);
                    }
                    break;
                }
            }
            // 上游流结束但没有 [DONE]/finish_reason（部分兼容上游不带）：补收尾
            if (st.messageStartSent && !st.finished) {
                // 统一关块：当前 text/thinking 块 + 所有未闭合的并行工具块（漏关任一 index Claude Code 会挂在块上）
                for (String e : AnthropicConverter.closeOpenBlocks(st)) {
                    out.write((e.endsWith("\n\n") ? e : e + "\n").getBytes(StandardCharsets.UTF_8));
                }
                out.write(("event: message_delta\ndata: " + Json.write(Json.obj("type", "message_delta",
                        // 流内见过 tool_calls 时必须报 tool_use：恒发 end_turn 会让 Claude Code
                        // 以为"回答完毕"不去执行工具 → 工具已生成却被静默收尾（早停真凶之一）
                        "delta", Json.obj("stop_reason", streamSawToolUse ? "tool_use" : "end_turn",
                                "stop_sequence", null),
                        "usage", Json.obj("output_tokens", Math.max(1, st.outputTokens)))) + "\n\n")
                        .getBytes(StandardCharsets.UTF_8));
                out.write("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
            // 权威用量取自转换器：它对每个 chunk（含末尾 choices 为空的 usage-only chunk）都累加进 st。
            // 上面逐行解析的局部 usage 只在"非空事件行"里取，usage-only chunk 因 events 空被 continue 跳过，
            // 导致 claude-code 流式落库 tokens=0+0（同 OpenAI 端 393~396 注释记录的坑）。st 恒不劣于逐行解析，故覆盖。
            if (st.usageAcc.get("prompt_tokens") != null || st.outputTokens > 0) {
                usage = Json.obj("prompt_tokens", st.usageAcc.getOrDefault("prompt_tokens", 0L),
                        "completion_tokens", st.outputTokens);
            }
            Long ttft = firstTokenAt == null ? null
                    : ForwardEngine.streamTtftMs(r, Long.parseLong(firstTokenAt));
            recordSuccess(c, model, ttft, usage, attemptNo, profile, System.currentTimeMillis() - startMs, client, compress);
            // 流式输出质量自反馈（同非流式：截断/空回复软失败降权 + 早停嫌疑）
            judgeStreamQuality(c, req, streamFinish, streamSawContent, streamSawToolUse, usage,
                    streamContentLen, client, compress);
        } catch (IOException e) {
            // try 块内 IOException 只可能来自向客户端写出（读侧异常被 pollLine 吞成 EOF）→ 客户端断开
            log.warn("[anthropic] 客户端断开(流式) {}/{}: {}", c.providerId(), c.model(), e.getMessage());
            statsService.logRequest(model, c.providerId(), c.model(), null,
                    (int) (System.currentTimeMillis() - startMs), null, null, "CLIENT_ABORTED", attemptNo, profile, e.getMessage(), client,
                    compress, null, null, null, c.channelId());
        }
    }

    /** 心跳哨兵：pollLine 超时返回（区别于 null=EOF） */
    private static final String HEARTBEAT_TIMEOUT = new String("gw-heartbeat-timeout");

    /** 带超时的 readLine（readLine 阻塞无法中断，ready() 轮询）；超时 HEARTBEAT_TIMEOUT / EOF null */
    private static String pollLine(BufferedReader reader, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        try {
            while (true) {
                if (reader.ready()) return reader.readLine();
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) return HEARTBEAT_TIMEOUT;
                Thread.sleep(Math.min(25, remain));
            }
        } catch (IOException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return HEARTBEAT_TIMEOUT;
        }
    }

    /** 胜者候选：对冲备选胜出时用 ForwardResult 内身份（统计/记账与真实服务方一致） */
    private static PolicyService.Candidate winnerOf(ForwardEngine.ForwardResult r, PolicyService.Candidate fallback) {
        if (r.providerId == null) return fallback;
        if (r.providerId.equals(fallback.providerId()) && r.channelId != null
                && r.channelId.equals(fallback.channelId())) return fallback;
        return new PolicyService.Candidate(r.providerId, r.model, r.channelId, fallback.weight(), fallback.groupPriority());
    }

    private void recordSuccess(PolicyService.Candidate c, String model, Long ttftMs,
                               Map<String, Object> usage, int attempts, String profile, long totalMs, String client,
                               String compress) {
        recordSuccess(c, model, ttftMs, usage, attempts, profile, totalMs, client, compress, "OK");
    }

    private void recordSuccess(PolicyService.Candidate c, String model, Long ttftMs,
                               Map<String, Object> usage, int attempts, String profile, long totalMs, String client,
                               String compress, String status) {
        Long in = null, out = null;
        if (usage != null) {
            in = Json.longVal(usage.get("prompt_tokens"));
            out = Json.longVal(usage.get("completion_tokens"));
        }
        policyService.recordSuccess(c.providerId(), c.model(), c.channelId(),
                ttftMs, in, out, false, client, Math.max(1, totalMs));
        statsService.logRequest(model, c.providerId(), c.model(), ttftMs == null ? null : (int) (long) ttftMs,
                (int) totalMs, in, out, status, attempts, profile,
                "CLIENT_ABORTED".equals(status) ? "client disconnected before delivery" : null, client,
                compress, null, null, null, c.channelId());
    }

    /** 网关侧直接拒绝（未转发上游）落 request_log：REJECTED（同 OpenAI 端点） */
    private void logRejected(String model, String client, String profile, String reason) {
        // 网关侧拒绝：从未选路，没有 Key 归属（channelId 落 NULL）
        statsService.logRequest(model == null || model.isBlank() ? "-" : model, null, null,
                null, null, null, null, "REJECTED", 0, profile, reason, client,
                null, null, GatewayController.rejectStatus(reason), null, null);
    }

    /** 输出质量自反馈（同 GatewayController）：截断/空回复/JSON不合法记软失败，截断同时上报输出预算学习；
     *  前置早停嫌疑判定（client×模型：压缩后无工具短回复 → 10 分钟惩罚） */
    private void judgeOutputQuality(PolicyService.Candidate c, Map<String, Object> req,
                                    Map<String, Object> parsedBody, String client, String compressTag) {
        try {
            String finish = null, content = null;
            int completionTokens = 0;
            List<Object> choices = Json.asList(parsedBody.get("choices"));
            Map<String, Object> usage = Json.asMap(parsedBody.get("usage"));
            if (usage != null) completionTokens = Json.longVal(usage.get("completion_tokens")).intValue();
            if (choices != null && !choices.isEmpty()) {
                Map<String, Object> first = Json.asMap(choices.get(0));
                finish = Json.str(first.get("finish_reason"));
                Map<String, Object> msg = Json.asMap(first.get("message"));
                Object cobj = msg == null ? null : msg.get("content");
                if (cobj instanceof String s) content = s;
                else if (cobj instanceof List<?> parts) {
                    StringBuilder sb = new StringBuilder();
                    for (Object o : parts) {
                        Map<String, Object> pm = Json.asMap(o);
                        if (pm != null && pm.get("text") instanceof String t) sb.append(t);
                    }
                    content = sb.toString();
                }
            }
            boolean toolCallsSeen = false;
            if (choices != null && !choices.isEmpty()) {
                Map<String, Object> msg0 = Json.asMap(Json.asMap(choices.get(0)).get("message"));
                toolCallsSeen = msg0 != null && msg0.get("tool_calls") != null;
            }
            policyService.judgeEarlyStop(client, c.providerId(), c.model(), compressTag,
                    RequestCategoryAnalyzer.requiresTools(req), toolCallsSeen,
                    content == null ? 0 : content.trim().length());
            if ("length".equalsIgnoreCase(finish)) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                int reqMt = Json.longVal(req.get("max_tokens")).intValue();
                policyService.recordOutputTruncation(c.providerId(), c.model(), completionTokens, reqMt);
                log.info("[anthropic] 输出质量反馈: {}/{} 截断(output={}tok)", c.providerId(), c.model(), completionTokens);
                return;
            }
            boolean hasToolCalls = toolCallsSeen;
            if (!hasToolCalls && (content == null || content.isBlank())) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                log.info("[anthropic] 输出质量反馈: {}/{} 空回复", c.providerId(), c.model());
                return;
            }
            Object rf = req.get("response_format");
            boolean wantJson = false;
            if (rf instanceof Map<?, ?> m) wantJson = "json_object".equals(m.get("type"));
            if (wantJson && !hasToolCalls && !isValidJson(content)) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                log.info("[anthropic] 输出质量反馈: {}/{} JSON不合法", c.providerId(), c.model());
                return;
            }
            policyService.recordQualityOk(c.providerId(), c.model());
        } catch (Exception e) {
            // 判断逻辑异常绝不干扰主流程
        }
    }

    private static boolean isValidJson(String s) {
        if (s == null || s.isBlank()) return false;
        try {
            Json.parseObject(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** 流式输出质量自反馈（同 GatewayController）：截断记软失败+输出预算学习（防毒化），
     *  空回复（无内容且非工具调用）记软失败；JSON 合法性流式不判。 */
    private void judgeStreamQuality(PolicyService.Candidate c, Map<String, Object> req, String finish,
                                    boolean sawContent, boolean sawToolUse, Map<String, Object> usage,
                                    long contentChars, String client, String compressTag) {
        try {
            // 早停嫌疑（与非流式同规则）
            policyService.judgeEarlyStop(client, c.providerId(), c.model(), compressTag,
                    RequestCategoryAnalyzer.requiresTools(req), sawToolUse, (int) Math.min(contentChars, Integer.MAX_VALUE));
            if ("length".equalsIgnoreCase(finish)) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                int completionTokens = usage == null ? 0 : Json.longVal(usage.get("completion_tokens")).intValue();
                int reqMt = Json.longVal(req.get("max_tokens")).intValue();
                policyService.recordOutputTruncation(c.providerId(), c.model(), completionTokens, reqMt);
                log.info("[anthropic] 输出质量反馈(流): {}/{} 截断(finish=length, output={}tok, 请求预算={})",
                        c.providerId(), c.model(), completionTokens, reqMt);
                return;
            }
            if (!sawContent && !sawToolUse) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                log.info("[anthropic] 输出质量反馈(流): {}/{} 空回复", c.providerId(), c.model());
                return;
            }
            policyService.recordQualityOk(c.providerId(), c.model());
        } catch (Exception e) {
            // 判断逻辑异常绝不干扰主流程
        }
    }

    // ================= 鉴权 =================

    /** Anthropic 客户端用 x-api-key；兼容 Bearer。两处取 Key 与 gateway-keys 比对。
     *  优先级统一为 Bearer 优先、x-api-key 其次（与 GatewayController.authenticate 一致，
     *  避免客户端同时带两个 header 时两种端点取到不同 key）。 */
    private boolean authError(String auth, String apiKeyHeader) {
        String key = null;
        if (auth != null && auth.startsWith("Bearer ")) key = auth.substring(7).trim();
        else if (apiKeyHeader != null && !apiKeyHeader.isBlank()) key = apiKeyHeader.trim();
        if (key == null) return true;
        String hash = ConfigService.sha256(key);
        return !configService.snapshot().gatewayKeyHashes.containsKey(hash);
    }

    /** UTF-8 写出（Writer 默认 ISO-8859-1 会把中文写成?，统一走字节流） */
    private void writeUtf8(HttpServletResponse response, String text) throws IOException {
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.getOutputStream().write(text.getBytes(StandardCharsets.UTF_8));
    }
}