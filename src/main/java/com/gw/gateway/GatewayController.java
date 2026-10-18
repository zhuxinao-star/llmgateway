package com.gw.gateway;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.forward.ContextCompressor;
import com.gw.forward.ForwardEngine;
import com.gw.forward.HedgedExecutor;
import com.gw.policy.PolicyService;
import com.gw.policy.RequestCategoryAnalyzer;
import com.gw.provider.adaptor.Json;
import com.gw.provider.adaptor.ProtocolAdaptor;
import com.gw.router.RouterService;
import com.gw.stats.StatsService;
import com.gw.util.ClientDetector;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 接入层：对外 OpenAI 兼容接口。
 * POST /v1/chat/completions  GET /v1/models
 *
 * 流式响应直接写 HttpServletResponse（不走消息转换器，避免类型路由问题）。
 */
@RestController
public class GatewayController {

    private static final Logger log = LoggerFactory.getLogger(GatewayController.class);
    /** 模型不存在时的回落组（保证工具侧配置一次即可，永不因模型名失效而报错） */
    private static final String DEFAULT_FALLBACK_MODEL = "free-fast";

    private final ConfigService configService;
    private final RouterService routerService;
    private final PolicyService policyService;
    private final ForwardEngine forwardEngine;
    private final HedgedExecutor hedgedExecutor;
    private final StatsService statsService;
    private final com.gw.forward.ContextCompressor contextCompressor;
    private final ClientQueueService clientQueueService;
    private final com.gw.policy.RankTraceStore rankTraceStore;

    public GatewayController(ConfigService configService, RouterService routerService,
                            PolicyService policyService, ForwardEngine forwardEngine,
                            HedgedExecutor hedgedExecutor, StatsService statsService,
                            com.gw.forward.ContextCompressor contextCompressor,
                            ClientQueueService clientQueueService,
                            com.gw.policy.RankTraceStore rankTraceStore) {
        this.configService = configService;
        this.routerService = routerService;
        this.policyService = policyService;
        this.forwardEngine = forwardEngine;
        this.hedgedExecutor = hedgedExecutor;
        this.statsService = statsService;
        this.contextCompressor = contextCompressor;
        this.clientQueueService = clientQueueService;
        this.rankTraceStore = rankTraceStore;
    }

    // ================= /v1/models =================

    @GetMapping("/v1/models")
    public ResponseEntity<?> models(@RequestHeader(value = "Authorization", required = false) String auth,
                                    @RequestHeader(value = "x-api-key", required = false) String apiKeyHeader) {
        ResponseEntity<?> authErr = authenticate(auth, apiKeyHeader);
        if (authErr != null) return authErr;
        return ResponseEntity.ok(Map.of("object", "list", "data", routerService.aggregatedModels()));
    }

    // ================= /v1/chat/completions =================

    @PostMapping("/v1/chat/completions")
    public void chat(@RequestHeader(value = "Authorization", required = false) String auth,
                     @RequestHeader(value = "x-api-key", required = false) String apiKeyHeader,
                     @RequestHeader(value = "X-Gateway-Profile", required = false) String profile,
                     @RequestHeader(value = "User-Agent", required = false) String userAgent,
                     @RequestBody String body,
                     HttpServletRequest request,
                     HttpServletResponse response) throws IOException {
        String client = ClientDetector.detect(userAgent);
        com.gw.stats.RequestTrace.start(response);   // 早于鉴权生成：REJECTED 行也带 trace
        ResponseEntity<?> authErr = authenticate(auth, apiKeyHeader);
        if (authErr != null) {
            logRejected("-", client, null, "unauthorized 401");
            writeJson(response, authErr); return;
        }
        boolean critical = "critical".equalsIgnoreCase(profile);

        Map<String, Object> req;
        try {
            req = Json.parseObject(body);
        } catch (Exception e) {
            logRejected("-", client, profile, "bad_request: 请求体不是合法 JSON");
            writeJson(response, openAiError(400, "invalid_request_error", "请求体不是合法 JSON: " + e.getMessage(), "bad_request"));
            return;
        }
        String model = Json.str(req.get("model"));
        if (model == null || model.isBlank()) {
            logRejected("-", client, profile, "bad_request: model 字段缺失");
            writeJson(response, openAiError(400, "invalid_request_error", "model 字段缺失", "bad_request"));
            return;
        }
        boolean stream = Json.boolVal(req.get("stream"), false);

        // ---- 路由解析（模型不存在时回落默认组 free-fast，保证工具侧总有可用模型） ----
        List<PolicyService.Candidate> candidates = routerService.resolve(model);
        if (candidates.isEmpty() && !model.equals(DEFAULT_FALLBACK_MODEL)) {
            List<PolicyService.Candidate> fb = routerService.resolve(DEFAULT_FALLBACK_MODEL);
            if (!fb.isEmpty()) {
                log.warn("模型 {} 不存在，回落默认组 {}", model, DEFAULT_FALLBACK_MODEL);
                candidates = fb;
            }
        }
        if (candidates.isEmpty()) {
            logRejected(model, client, profile, "model_not_found 404");
            writeJson(response, openAiError(404, "invalid_request_error", "模型不存在: " + model + "（等待目录刷新或检查配置）", "model_not_found"));
            return;
        }

        // ---- 上下文自动压缩：Agent 工具每轮背全部历史，输入随轮次膨胀（实测 164K）。
        // 超阈值时先压成摘要再路由——压缩后的大小参与 contextWindow 过滤与评分。
        // 压缩失败原样转发（ContextCompressor 内部兜底，永不影响请求成功率）
        ContextCompressor.Compressed cmp = contextCompressor.maybeCompress(req, client);
        if (cmp.compressed()) response.setHeader("X-Gateway-Context-Compressed", "true");

        // ---- 策略排序（含语义路由：请求内容判定类别，擅长该类别的模型加分） ----
        String strategy = configService.snapshot().config.getStrategy();
        String requestCategory = RequestCategoryAnalyzer.analyze(req);
        int estIn = cmp.compressed() ? cmp.tokensAfter() : RequestCategoryAnalyzer.estimateInputTokens(req);
        // 决策留痕：按采样抽中才建收集器（未抽中传 null，rank 不分配因子表）。
        // 留痕只记"这一发怎么选的"，主选若失败转兜底，实际承接者看同 traceId 的 request_log 行。
        PolicyService.RankTrace rt = rankTraceStore.begin(com.gw.stats.RequestTrace.get(), model, client, "/v1/chat/completions");
        List<PolicyService.Scored> ranked = policyService.rank(candidates, strategy, critical, requestCategory,
                estIn, RequestCategoryAnalyzer.requiresTools(req), client, rt);
        if (rt != null) rankTraceStore.commit(rt);
        if (ranked.isEmpty()) {
            logRejected(model, client, profile, "no_candidate 503" + tierHint(strategy));
            writeJson(response, openAiError(503, "gateway_error", "所有候选渠道均不可用" + tierHint(strategy) + ": " + model, "no_candidate"));
            return;
        }

        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        long deadline = System.nanoTime() + (long) d.getTotalBudgetMs() * 1_000_000;

        // ---- 下游 SSE 保活：仅流式且 interval>0 时创建，但**延迟到 streamToClient 真正开始吐流时才
        //  commit(200)+start**（见 Fix A）。开局前（排队/响应头/TTFT 等待）的硬失败仍走真实 HTTP
        //  429/503，好让工具按退避重试——提前 commit 会把可重试的 429 降级成"200+流内 error"，
        //  Claude Code 对流内错误不重试，这正是"整轮不回复"的根因。非流式或 interval<=0 时 ka=null。 ----
        SseKeepAlive ka = (stream && d.getSseKeepAliveIntervalMs() > 0)
                ? new SseKeepAlive(response, d.getSseKeepAliveIntervalMs()) : null;

        // ---- 同源并发队列：同一 client 的并发请求（默认>2）排队等上一个完成转发（含流式传完），
        //  从根上避免一个工具高频连发把免费上游 TPM/RPM 一击打爆。排队超时放行撞墙兜底，绝不阻塞。 ----
        ClientQueueService.QueueToken q = clientQueueService.acquire(client);
        try {
            ForwardEngine.ForwardResult lastFail = null;
            long requestStart = System.nanoTime();
            // 落库时长只认这一把钟：total_ms 与 ttft_ms 必须同源，否则"总时长比首字还短"会结构性出现
            //（旧流式路径从 streamToClient 起算，把 execute 里的上游首字等待整段漏掉，307 行实证）。
            final long requestStartMs = System.currentTimeMillis();
            int attemptNo = 0;
            // ---- Fix C：整单在 totalBudget 内做有限轮"耐心 failover"。首轮全败且失败属可重试类
            //  （429/5xx/超时/网络）时，退避（上游 Retry-After 优先，无则固定小退避）后按 pass1 学到的
            //  最新冷却/熔断重排名、避开刚限流的家再来一轮——把免费上游的瞬时限流透明扛住；非重试类
            //  （其它 4xx）立即放弃回真实状态码。封顶 2 轮，绝不盲目狂刷加剧限流。 ----
            List<PolicyService.Scored> passRanked = ranked;
            final int maxPasses = 2;
            for (int pass = 1; pass <= maxPasses; pass++) {
                // 每轮候选数：钳到该轮 ranked 大小（点名单渠道时不越界）与配置 maxAttempts（≤6）
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
                    // 之后 streamToClient 排空最长 120s 却不占额度 → 免费上游的 concurrency limit 被击穿
                    // （日志实证 concFull×10 全在流式阶段）。STREAM 时把该候选的并发位一直持有到排空结束。
                    boolean holdForStream = false;
                    try {
                        Map<String, Object> forwardReq = new LinkedHashMap<>(req);
                        // 输出 token 预算学习：按该候选学到的单请求最大安全输出钳制 max_tokens，
                        // 避免请求的 max_tokens 远超该家实际上限被截断（finish_reason=length）白烧
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
                            // Retry-After 精确冷却：上游给了具体秒数则用它（取与固定冷却的更长值）
                            if ((r.status == 429 || r.status == 503) && r.retryAfterSeconds > 0) {
                                policyService.recordRateLimitWithRetryAfter(c.providerId(), c.channelId(), r.retryAfterSeconds);
                            }
                            statsService.logRequest(model, c.providerId(), c.model(), null, null,
                                    null, null, "FAILED", attemptNo, profile, r.error, client, cmp.compressTag(),
                                    null, r.status, null, c.channelId());
                            log.warn("转发失败 attempt={} pass={} client={} provider={}/{} ch={} status={} err={}",
                                    attemptNo, pass, client, c.providerId(), c.model(), c.channelId(), r.status, r.error);
                            lastFail = r;
                        }
                        case SUCCESS -> {
                            // 非流式：一次性返回（对冲只对流式生效，胜者即主选）
                            Map<String, Object> usage = Json.asMap(r.parsedBody.get("usage"));
                            judgeOutputQuality(c, req, r.parsedBody, client, cmp.compressTag());   // 输出质量自反馈（截断/空回复/JSON不合法/早停）
                            response.setStatus(200);
                            response.setContentType("application/json");
                            response.setHeader("X-Gateway-Channel", c.providerId() + "/" + c.channelId());
                            response.setHeader("X-Gateway-Attempt", String.valueOf(attemptNo));
                            String delivered = "OK";
                            try {
                                writeUtf8(response, Json.write(r.parsedBody));
                            } catch (IOException e) {
                                // 上游已产出结果但工具端已断开——"日志全绿、工具没收到数据"的真相记录点
                                delivered = "CLIENT_ABORTED";
                                log.warn("客户端提前断开(非流式) {}/{}: {}", c.providerId(), c.model(), e.getMessage());
                            }
                            recordSuccessAndLog(c, model, r.ttftMs / 1_000_000, usage, delivered, attemptNo, profile,
                                    (System.nanoTime() - requestStart) / 1_000_000, client, cmp.compressTag());
                            return;
                        }
                        case STREAM -> {
                            // 胜者可能是对冲备选：以 ForwardResult 内身份为准（统计/日志/响应头一致）
                            PolicyService.Candidate winner = winnerOf(r, c);
                            // #11 并发位一直占到流排空结束（execute 已保留 c 的额度未释放，异常也要还）
                            try {
                                streamToClient(model, winner, r, attemptNo, profile, client, req, response, cmp.compressTag(), ka, requestStartMs);
                            } finally {
                                policyService.releaseInflight(c.providerId(), c.model());
                            }
                            return;
                        }
                    }
                }
                // ---- 一轮（全候选）结束仍未成功：仅当最后失败可重试时才再来一轮 ----
                if (pass >= maxPasses) break;
                long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
                long backoffMs = patientBackoffMs(lastFail);
                if (backoffMs <= 0 || backoffMs >= remainingMs) break;   // 非重试类/无退避价值/预算不够=交客户端退避
                log.info("候选首轮全败(可重试)，网关退避 {}ms 后按最新健康度重扫一轮 model={} client={}",
                        backoffMs, model, client);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                // 重排名：pass1 期间 recordFailure 写入的冷却/熔断即刻生效，第二轮优先落到恢复/未被限流的家
                List<PolicyService.Scored> reRanked = policyService.rank(candidates, strategy, critical,
                        requestCategory, estIn, RequestCategoryAnalyzer.requiresTools(req), client);
                if (reRanked.isEmpty()) break;
                passRanked = reRanked;
            }

            // ---- 候选穷尽：透传最后上游错误（真实 HTTP 状态码，供工具退避重试）。
            //  走到这里说明没有任何候选返回 STREAM，ka 尚未 commit，响应头/状态码仍可写。 ----
            if (lastFail != null && lastFail.status > 0) {
                response.setStatus(lastFail.status);
                response.setContentType("application/json");
                response.setHeader("X-Gateway-Error", "all_attempts_failed");
                writeUtf8(response, lastFail.body == null ? "" : lastFail.body);
                return;
            }
            writeJson(response, openAiError(504, "gateway_error", "全部候选尝试失败（超时/网络错误）: " + model, "all_attempts_failed"));
        } finally {
            if (ka != null) ka.stop();
            clientQueueService.release(q);
            com.gw.stats.RequestTrace.clear();
        }
    }

    // ================= SSE 透传 + 心跳看门狗 =================

    private void streamToClient(String model, PolicyService.Candidate c,
                                 ForwardEngine.ForwardResult r, int attemptNo,
                                 String profile, String client, Map<String, Object> req,
                                 HttpServletResponse response, String compress, SseKeepAlive ka,
                                 long requestStartMs) throws IOException {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        // relayStartMs 只用来量"这段流在网关手里活了多久"（心跳窗口、首字偏移）；
        // 落库的 total_ms 必须从 requestStartMs 起算，才和 ttft_ms（含 execute 里的上游等待）同一把钟。
        long relayStartMs = System.currentTimeMillis();
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
        long lastRealAt = 0;   // 上一次"真内容行"（产出 token/事件）的时间；0=还没有
        // 出 token 后的流心跳窗口统一用配置的宽窗口（120s）：慢模型/长思考模型（流内事件
        // 间隔可达 60~100s）需保留宽窗口不误杀；不再按费用档分档（免费档 60s 曾误杀正常生成）。
        long afterTokenWinMs = d.getFirstTokenHeartbeatTimeoutMs();
        Map<String, Object> usage = null;
        BufferedReader reader = r.streamReader;
        ProtocolAdaptor adaptor = r.adaptor;
        java.io.OutputStream out = ka != null ? ka.guarded() : response.getOutputStream();
        // 流式质量信号收集（收尾统一判定软失败：截断/空回复）
        String streamFinish = null;
        boolean streamSawContent = false, streamSawToolUse = false;
        long streamContentLen = 0;   // 累计内容字符数（早停嫌疑判定：压缩后仅回几个字=疑似静默早停）
        boolean sawDone = false;   // 上游发 [DONE] 即置 true（循环前声明，供提前 break 与收尾共用）
        boolean malformed = false; // 上游吐过残行（JSON 未闭合）→ 本流判失败，不发 [DONE]
        String pendingOut = null;  // 延迟一行透传：残行在解析关就被丢弃，不会成为 pendingOut
        try (reader; out) {
            // execute 内已读出的首行先当首事件处理；后续行全部走 pollLine（带心跳超时）
            String pending = r.firstLine;
            while (true) {
                String cur;
                if (pending != null) {          // 首行（对冲竞速保证拿到时才返回 STREAM）
                    cur = pending;
                    pending = null;
                } else {
                    // 窗口双档：首字前严格（30s，真死流快速发现）；已出 token 后统一用宽窗口（120s），防长思考模型误杀
                    long win = lastRealAt == 0 ? d.getStreamHeartbeatTimeoutMs() : afterTokenWinMs;
                    String next = pollLine(reader, win);
                    if (next == HEARTBEAT_TIMEOUT) {
                        long now = System.currentTimeMillis();
                        log.warn("流中心跳超时（{}ms 无事件），掐断 {}/{}",
                                lastRealAt == 0 ? d.getStreamHeartbeatTimeoutMs() : afterTokenWinMs,
                                c.providerId(), c.model());
                        statsService.logRequest(model, c.providerId(), c.model(), (int) (r.ttftMs / 1_000_000),
                                (int) (now - requestStartMs), null, null, "STREAM_TIMEOUT", attemptNo, profile, "heartbeat timeout", client,
                                compress, null, null, null, c.channelId());
                        policyService.recordFailure(c.providerId(), c.model(), c.channelId(), 0, "stream heartbeat timeout", client);
                        if (pendingOut != null) {      // 已收到的完整行仍透传；客户端可能已断，写失败仅忽略
                            try {
                                out.write((pendingOut + "\n\n").getBytes(StandardCharsets.UTF_8));
                                out.flush();
                            } catch (IOException ignore) {
                            }
                            pendingOut = null;
                        }
                        return;
                    }
                    if (next == null) break;    // 上游 EOF：收尾分支按"是否见过 [DONE]/finish_reason"判定完整/不完整
                    cur = next;
                }
                // SSE 语义：空行/注释行（: 开头）是 keep-alive，永远不是内容，跳过且不刷新窗口。
                boolean isKeepAliveLine = cur.isBlank() || cur.startsWith(":");
                if (isKeepAliveLine) {
                    // 死流兜底：keep-alive 行能骗过 pollLine 超时（pollLine 只在"完全无数据"时超时）。
                    // 若真内容窗口耗尽（如每秒一条注释行的死流），在这里主动掐断。
                    long win = lastRealAt == 0 ? d.getStreamHeartbeatTimeoutMs() : afterTokenWinMs;
                    if (System.currentTimeMillis() - (lastRealAt == 0 ? relayStartMs : lastRealAt) > win) {
                        long now = System.currentTimeMillis();
                        log.warn("流内长时间无真内容（keep-alive 行续命 {}ms），掐断 {}/{}",
                                now - (lastRealAt == 0 ? relayStartMs : lastRealAt), c.providerId(), c.model());
                        statsService.logRequest(model, c.providerId(), c.model(), (int) (r.ttftMs / 1_000_000),
                                (int) (now - requestStartMs), null, null, "STREAM_TIMEOUT", attemptNo, profile, "no real content window exceeded", client,
                                compress, null, null, null, c.channelId());
                        policyService.recordFailure(c.providerId(), c.model(), c.channelId(), 0, "stream no real content timeout", client);
                        if (pendingOut != null) {      // 已收到的完整行仍透传；客户端可能已断，写失败仅忽略
                            try {
                                out.write((pendingOut + "\n\n").getBytes(StandardCharsets.UTF_8));
                                out.flush();
                            } catch (IOException ignore) {
                            }
                            pendingOut = null;
                        }
                        return;
                    }
                    continue;
                }
                // data: [DONE] = 上游正常终止信号：透传给客户端后立刻 break，不再等 EOF。
                // 部分上游发完不关连接，死等会把正常结束误记成 STREAM_TIMEOUT。
                if (cur.strip().startsWith("data: [DONE]")) {
                    if (malformed) break;              // 流内已现残行：不发 [DONE]，收尾按失败记账
                    if (pendingOut != null) {          // 先补上挂起的最后一行（完整行），再发 [DONE]
                        out.write((pendingOut + "\n\n").getBytes(StandardCharsets.UTF_8));
                        pendingOut = null;
                    }
                    out.write((cur + "\n\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    sawDone = true;
                    break;
                }
                // 其余 data: 行一律透传给客户端并提取 usage/质量信号——包括 delta 空但带
                // finish_reason 的终止 chunk、以及带 usage 的 chunk（choices 可能为空）。
                // 这些 chunk 里没有非空 content/tool_calls，若沿用"只看真内容"会走 continue 被吞掉，
                // 导致客户端收不到 finish_reason/usage，报 "Stream ended without finish_reason"、tokens=0+0。
                List<String> events = adaptor.transformStreamEvent(cur);
                lastRealAt = System.currentTimeMillis();
                if (firstTokenAt == null) firstTokenAt = String.valueOf(lastRealAt - relayStartMs);
                for (String ol : events) {
                    // 残行防线：非 [DONE] 的 data: 行必须先解析成完整 JSON。上游中途断流时
                    // readLine 会把无换行的半截残行交出来——在这里直接丢弃、绝不下发（DSH/pi-ai
                    // 逐行 JSON.parse，残行必炸 "Unexpected end of JSON input"，2026-09-17 turn 6
                    // 实锤：AMD MiniCPM 生成中途断流），并标记整条流为失败。
                    Map<String, Object> ev = null;
                    if (ol.startsWith("data:") && !"[DONE]".equals(ol.substring(5).strip())) {
                        try {
                            ev = Json.parseObject(ol.substring(5).strip());
                        } catch (Exception e) {
                            log.warn("上游残行丢弃（JSON 未闭合，疑似中途断流）: {}",
                                    ol.substring(0, Math.min(80, ol.length())));
                            malformed = true;
                            continue;
                        }
                    }
                    // 提取 usage（流式最后一个含 usage 的事件）
                    if (ev != null) {
                        Map<String, Object> u = Json.asMap(ev.get("usage"));
                        if (u != null && (u.get("prompt_tokens") != null || u.get("completion_tokens") != null)) {
                            usage = u;
                        }
                        // 流式质量信号：finish_reason / 内容出现 / 工具调用
                        List<Object> chs = Json.asList(ev.get("choices"));
                        if (chs != null && !chs.isEmpty()) {
                            Map<String, Object> ch = Json.asMap(chs.get(0));
                            if (ch.get("finish_reason") != null) streamFinish = Json.str(ch.get("finish_reason"));
                            Map<String, Object> delta = Json.asMap(ch.get("delta"));
                            if (delta != null) {
                                if (delta.get("content") instanceof String t) {
                                    streamContentLen += t.length();
                                    if (!t.isBlank()) streamSawContent = true;
                                }
                                if (delta.get("tool_calls") != null) streamSawToolUse = true;
                            }
                        }
                    }
                    // 延迟一行透传：本行先挂起，下一行到达才透传上一行（到此的 ol 必为完整 JSON）
                    if (pendingOut != null) {
                        out.write((pendingOut + "\n\n").getBytes(StandardCharsets.UTF_8));
                    }
                    pendingOut = ol;
                }
                out.flush();
                // 若 transformStreamEvent 内联产生了 [DONE]（未单独成行），同样视为终止
                if (sawDone) break;
            }
            // 上游 EOF（没等到 [DONE]）分两种：
            //   · 完整流：已见过 finish_reason chunk → 补发挂起的最后一行，正常收尾记 OK。
            //   · 不完整流：无 [DONE] 也无 finish_reason = 上游中途断流（AMD 等免费上游会
            //     在生成中途杀连接）→ 丢弃半截残行、不补 [DONE]、不记成功。客户端拿到干净
            //     EOF 会按它自己的重试策略换路重发（DSH policyKey 含 TRANSPORT），网关侧记
            //     失败让该模型降权冷却。旧逻辑无条件记 OK 并把残行透传——DSH/pi-ai 逐行
            //     JSON.parse 直接炸 "Unexpected end of JSON input"（turn 6 实锤）。
            if (malformed || (!sawDone && streamFinish == null)) {
                pendingOut = null;   // 半截残行就此丢弃（此前各完整行已透传）
                long now = System.currentTimeMillis();
                log.warn("上游流不完整（无 [DONE]/finish_reason，疑似中途断流），按失败记账 {}/{}",
                        c.providerId(), c.model());
                Map<String, Object> partialUsage = usage != null ? usage : adaptor.extractUsage(null);
                statsService.logRequest(model, c.providerId(), c.model(), (int) (r.ttftMs / 1_000_000),
                        (int) (now - requestStartMs),
                        partialUsage == null ? null : Json.longVal(partialUsage.get("prompt_tokens")),
                        partialUsage == null ? null : Json.longVal(partialUsage.get("completion_tokens")),
                        "STREAM_INCOMPLETE", attemptNo, profile, "upstream stream ended without finish_reason", client,
                        compress, null, null, null, c.channelId());
                policyService.recordFailure(c.providerId(), c.model(), c.channelId(), 0,
                        "stream ended without finish_reason", client);
                return;
            }
            // 正常结束：把挂起的最后一行透传，再补收尾（如缺 [DONE]）
            if (pendingOut != null) {
                out.write((pendingOut + "\n\n").getBytes(StandardCharsets.UTF_8));
                pendingOut = null;
            }
            boolean doneSent = false;
            for (String fin : adaptor.streamFinish()) {
                out.write((fin + "\n\n").getBytes(StandardCharsets.UTF_8));
                if (fin.contains("[DONE]")) doneSent = true;
            }
            out.flush();
            // 成功统计
            Long ttft = firstTokenAt == null ? null
                    : ForwardEngine.streamTtftMs(r, Long.parseLong(firstTokenAt));
            Map<String, Object> finalUsage = usage != null ? usage : adaptor.extractUsage(null);
            recordSuccessAndLog(c, model, ttft, finalUsage, "OK", attemptNo, profile,
                    System.currentTimeMillis() - requestStartMs, client, compress);
            // 流式输出质量自反馈（同非流式：截断/空回复软失败降权 + 早停嫌疑）
            judgeStreamQuality(c, req, streamFinish, streamSawContent, streamSawToolUse, finalUsage,
                    streamContentLen, client, compress);
        } catch (IOException e) {
            // try 块内 IOException 只可能来自向客户端写出（上游读异常已被 pollLine 吞成 EOF）
            // → 语义是"客户端断开"，如实记 CLIENT_ABORTED，不再冒充模糊的 STREAM_ABORTED
            log.warn("客户端断开(流式) {}/{}: {}", c.providerId(), c.model(), e.getMessage());
            statsService.logRequest(model, c.providerId(), c.model(), null,
                    (int) (System.currentTimeMillis() - requestStartMs), null, null, "CLIENT_ABORTED", attemptNo, profile, e.getMessage(), client,
                    compress, null, null, null, c.channelId());
        }
    }

    /** 心跳哨兵：pollLine 超时返回（区别于 null=EOF） */
    private static final String HEARTBEAT_TIMEOUT = new String("gw-heartbeat-timeout");

    /**
     * 带超时的 readLine：ready() 轮询（readLine 阻塞无法中断）。
     * 窗口内读到行返回（可能为空行）；超时返回 HEARTBEAT_TIMEOUT；EOF 返回 null。
     */
    private static String pollLine(BufferedReader reader, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        try {
            while (true) {
                if (reader.ready()) {
                    String l = reader.readLine();
                    return l;   // 可能为空行/注释行——空行也当事件（SSE 分隔符），超时由外层窗口兜底
                }
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) return HEARTBEAT_TIMEOUT;
                Thread.sleep(Math.min(25, remain));
            }
        } catch (IOException e) {
            return null;   // 当 EOF：外层按中断/结束处理
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

    /** 可重试失败：网关超时、网络错误(status=0)、429 限流、5xx 服务端瞬时错——这些换家/稍后再试可能成。
     *  其它 4xx（400/401/403/404/422…）是请求本身的问题，重试无意义，立即回真实状态码给工具。 */
    static boolean isRetryableFailure(ForwardEngine.ForwardResult f) {
        if (f == null) return false;
        if (f.timeout) return true;
        int s = f.status;
        return s == 0 || s == 429 || s == 500 || s == 502 || s == 503;
    }

    /** 再来一轮前的退避毫秒：上游 Retry-After 优先（但不超 30s，更长不值得网关内联干等、交工具退避）；
     *  无 Retry-After 的可重试失败给一次固定小退避。返回 <=0 表示不重试。 */
    static long patientBackoffMs(ForwardEngine.ForwardResult f) {
        if (!isRetryableFailure(f)) return -1;
        if (f.retryAfterSeconds > 0) {
            long w = f.retryAfterSeconds * 1000L;
            return w > 30_000 ? -1 : Math.max(200, w);
        }
        return 800;
    }

    private void recordSuccessAndLog(PolicyService.Candidate c, String model, Long ttftMs,
                                     Map<String, Object> usage, String status, int attempts,
                                     String profile, long totalMs, String client, String compress) {
        Long in = null, out = null;
        if (usage != null) {
            in = Json.longVal(usage.get("prompt_tokens"));
            out = Json.longVal(usage.get("completion_tokens"));
        }
        policyService.recordSuccess(c.providerId(), c.model(), c.channelId(),
                ttftMs, in, out, false, client, totalMs);
        statsService.logRequest(model, c.providerId(), c.model(), ttftMs == null ? null : (int) (long) ttftMs,
                (int) totalMs, in, out, status, attempts, profile,
                "CLIENT_ABORTED".equals(status) ? "client disconnected before delivery" : null, client,
                compress, null, null, null, c.channelId());
    }

    /** 输出质量自反馈：识别软失败（截断/空回复/JSON不合法），按 provider×model 计数降权，
     *  让路由自动避开"经常抽风/截断"的家。质量正常则清零。
     *  截断事件同时上报输出预算学习（completion_tokens 作为该家单请求最大输出上限样本）。 */
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
            // 0) 早停嫌疑（独立于软失败判定）：无 tool_calls + 回复极短 + 本请求经历过压缩 → 惩罚；有工具调用 → 解除
            boolean toolCallsSeen = false;
            if (choices != null && !choices.isEmpty()) {
                Map<String, Object> msg0 = Json.asMap(Json.asMap(choices.get(0)).get("message"));
                toolCallsSeen = msg0 != null && msg0.get("tool_calls") != null;
            }
            policyService.judgeEarlyStop(client, c.providerId(), c.model(), compressTag,
                    RequestCategoryAnalyzer.requiresTools(req), toolCallsSeen,
                    content == null ? 0 : content.trim().length());
            // 1) 截断：记软失败 + 上报输出预算学习（防毒化：恰=用户小预算的不学）
            if ("length".equalsIgnoreCase(finish)) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                int reqMt = Json.longVal(req.get("max_tokens")).intValue();
                policyService.recordOutputTruncation(c.providerId(), c.model(), completionTokens, reqMt);
                log.info("输出质量反馈: {}/{} 截断(finish_reason=length, output={}tok)", c.providerId(), c.model(), completionTokens);
                return;
            }
            // 2) 空回复（非工具调用场景）
            boolean hasToolCalls = false;
            if (choices != null && !choices.isEmpty()) {
                Map<String, Object> msg = Json.asMap(Json.asMap(choices.get(0)).get("message"));
                hasToolCalls = msg != null && msg.get("tool_calls") != null;
            }
            if (!hasToolCalls && (content == null || content.isBlank())) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                log.info("输出质量反馈: {}/{} 空回复", c.providerId(), c.model());
                return;
            }
            // 3) 要求 JSON 但返回不合法 JSON
            Object rf = req.get("response_format");
            boolean wantJson = false;
            if (rf instanceof Map<?, ?> m) wantJson = "json_object".equals(m.get("type"));
            if (wantJson && !hasToolCalls && !isValidJson(content)) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                log.info("输出质量反馈: {}/{} JSON不合法", c.providerId(), c.model());
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

    /** 流式输出质量自反馈：流式是主流量（zcode/claude-code），与非流式同规则但按收集的信号判定。
     *  截断（finish_reason=length）记软失败 + 输出预算学习（防毒化：恰=用户小预算的不学）；
     *  空回复（无内容且非工具调用）记软失败。
     *  JSON 合法性流式不判（增量拼接代价高且非流式已覆盖 response_format 场景）。 */
    private void judgeStreamQuality(PolicyService.Candidate c, Map<String, Object> req, String finish,
                                    boolean sawContent, boolean sawToolUse, Map<String, Object> usage,
                                    long contentChars, String client, String compressTag) {
        try {
            // 早停嫌疑（与非流式同规则）：先记再走软失败分支
            policyService.judgeEarlyStop(client, c.providerId(), c.model(), compressTag,
                    RequestCategoryAnalyzer.requiresTools(req), sawToolUse, (int) Math.min(contentChars, Integer.MAX_VALUE));
            if ("length".equalsIgnoreCase(finish)) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                int completionTokens = usage == null ? 0 : Json.longVal(usage.get("completion_tokens")).intValue();
                int reqMt = Json.longVal(req.get("max_tokens")).intValue();
                policyService.recordOutputTruncation(c.providerId(), c.model(), completionTokens, reqMt);
                log.info("输出质量反馈(流): {}/{} 截断(finish=length, output={}tok, 请求预算={})",
                        c.providerId(), c.model(), completionTokens, reqMt);
                return;
            }
            if (!sawContent && !sawToolUse) {
                policyService.recordQualityIssue(c.providerId(), c.model());
                log.info("输出质量反馈(流): {}/{} 空回复", c.providerId(), c.model());
                return;
            }
            policyService.recordQualityOk(c.providerId(), c.model());
        } catch (Exception e) {
            // 判断逻辑异常绝不干扰主流程
        }
    }

    // ================= 鉴权与错误模型 =================

    // ================= /v1/images/generations（图像生成透传） =================

    /**
     * OpenAI Images 端点透传：按请求的 model 走智能路由（与聊天同一套候选/评分/failover），
     * 转发到候选供应商的 {baseUrl}/images/generations，非流式 JSON 原样返回。
     * 支持 image 类模型（如 agnes-image-2.1-flash：上游拒绝走 chat/completions，必须此端点）。
     */
    @PostMapping("/v1/images/generations")
    public void images(@RequestHeader(value = "Authorization", required = false) String auth,
                       @RequestHeader(value = "x-api-key", required = false) String apiKeyHeader,
                       @RequestHeader(value = "User-Agent", required = false) String userAgent,
                       @RequestBody String body,
                       HttpServletResponse response) throws IOException {
        String client = ClientDetector.detect(userAgent);
        com.gw.stats.RequestTrace.start(response);
        ResponseEntity<?> authErr = authenticate(auth, apiKeyHeader);
        if (authErr != null) {
            logRejected("-", client, null, "unauthorized 401");
            writeJson(response, authErr); return;
        }

        Map<String, Object> req;
        try {
            req = Json.parseObject(body);
        } catch (Exception e) {
            logRejected("-", client, null, "bad_request: 请求体不是合法 JSON");
            writeJson(response, openAiError(400, "invalid_request_error", "请求体不是合法 JSON: " + e.getMessage(), "bad_request"));
            return;
        }
        String model = Json.str(req.get("model"));
        if (model == null || model.isBlank()) {
            logRejected("-", client, null, "bad_request: model 字段缺失");
            writeJson(response, openAiError(400, "invalid_request_error", "model 字段缺失", "bad_request"));
            return;
        }

        List<PolicyService.Candidate> candidates = routerService.resolve(model);
        if (candidates.isEmpty() && !model.equals(DEFAULT_FALLBACK_MODEL)) {
            List<PolicyService.Candidate> fb = routerService.resolve(DEFAULT_FALLBACK_MODEL);
            // 图像端点不盲目回落 free-fast（文本组模型生成不了图）：仅当点名模型不存在时提示
            if (fb.isEmpty()) {
                logRejected(model, client, null, "model_not_found 404");
                writeJson(response, openAiError(404, "invalid_request_error", "模型不存在: " + model, "model_not_found"));
                return;
            }
            logRejected(model, client, null, "model_not_found 404(图像模型需点名)");
            writeJson(response, openAiError(404, "invalid_request_error",
                    "模型不存在: " + model + "（图像模型需在对应供应商下配置，如 agnes/agnes-image-2.1-flash）", "model_not_found"));
            return;
        }
        if (candidates.isEmpty()) {
            logRejected(model, client, null, "model_not_found 404");
            writeJson(response, openAiError(404, "invalid_request_error", "模型不存在: " + model, "model_not_found"));
            return;
        }
        String strategy = configService.snapshot().config.getStrategy();
        List<PolicyService.Scored> ranked = policyService.rank(candidates, strategy, false, null,
                RequestCategoryAnalyzer.estimateInputTokens(req));
        if (ranked.isEmpty()) {
            logRejected(model, client, null, "no_candidate 503" + tierHint(strategy));
            writeJson(response, openAiError(503, "gateway_error", "所有候选渠道均不可用" + tierHint(strategy) + ": " + model, "no_candidate"));
            return;
        }

        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        long deadline = System.nanoTime() + (long) d.getTotalBudgetMs() * 1_000_000;
        // 候选数 < 配置值时钳到候选数（单候选模型若用 max(maxAttempts,size) 会 ranked.get(1) 越界 500）
        int maxAttempts = Math.min(Math.max(1, ranked.size()), Math.max(d.getMaxAttempts(), 1));
        maxAttempts = Math.min(maxAttempts, 6);

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (System.nanoTime() > deadline) break;
            PolicyService.Candidate c = ranked.get(attempt).candidate;
            policyService.tryEnterHalfOpen(c.providerId(), c.model());
            policyService.acquireInflight(c.providerId(), c.model());
            policyService.recordChannelUsed(c.providerId(), c.channelId());
            ForwardEngine.ForwardResult r;
            try {
                r = forwardEngine.executeImages(req, c, deadline);
            } finally {
                policyService.releaseInflight(c.providerId(), c.model());
            }
            if (r.kind == ForwardEngine.ForwardResult.Kind.FAILED) {
                policyService.recordFailure(c.providerId(), c.model(), c.channelId(), r.status, r.error, client);
                statsService.logRequest(model, c.providerId(), c.model(), null, null,
                        null, null, "FAILED", attempt + 1, null, r.error, client, null, null, r.status, null, c.channelId());
                log.warn("图像转发失败 attempt={} client={} provider={}/{} status={} err={}",
                        attempt + 1, client, c.providerId(), c.model(), r.status, r.error);
                continue;
            }
            // SUCCESS：原样透传上游 JSON（含 data[].url / b64_json）
            policyService.recordSuccess(c.providerId(), c.model(), c.channelId(),
                    r.ttftMs / 1_000_000, null, null, false, client, r.ttftMs / 1_000_000);
            statsService.logRequest(model, c.providerId(), c.model(),
                    (int) (r.ttftMs / 1_000_000), (int) (r.ttftMs / 1_000_000),
                    null, null, "OK", attempt + 1, null, null, client, null, null, null, null, c.channelId());
            response.setStatus(200);
            response.setContentType("application/json");
            response.setHeader("X-Gateway-Channel", c.providerId() + "/" + c.channelId());
            response.setHeader("X-Gateway-Attempt", String.valueOf(attempt + 1));
            writeUtf8(response, r.body == null ? "{}" : r.body);
            return;
        }
        writeJson(response, openAiError(504, "gateway_error", "图像生成全部候选失败: " + model, "all_attempts_failed"));
    }

    // ================= /v1/embeddings + /v1/videos（多模态透传） =================

    /** 多模态透传端点通用转发：候选/评分/failover/熔断/限流学习与聊天同一套。
     *  model 缺失或无候选时不盲目回落 free-fast（文本组模型干不了 embedding/视频）。 */
    private void forwardRawEndpoint(Map<String, Object> req, String model, String client, String what,
                                    java.util.function.BiFunction<Map<String, Object>, PolicyService.Candidate, ForwardEngine.ForwardResult> exec,
                                    HttpServletResponse response) throws IOException {
        List<PolicyService.Candidate> candidates = routerService.resolve(model);
        if (candidates.isEmpty()) {
            logRejected(model, client, null, "model_not_found 404(" + what + ")");
            writeJson(response, openAiError(404, "invalid_request_error",
                    "模型不存在: " + model + "（" + what + "需在对应供应商下配置专门模型）", "model_not_found"));
            return;
        }
        String strategy = configService.snapshot().config.getStrategy();
        List<PolicyService.Scored> ranked = policyService.rank(candidates, strategy, false, null,
                RequestCategoryAnalyzer.estimateInputTokens(req));
        if (ranked.isEmpty()) {
            logRejected(model, client, null, "no_candidate 503(" + what + ")" + tierHint(strategy));
            writeJson(response, openAiError(503, "gateway_error", "所有候选渠道均不可用" + tierHint(strategy) + ": " + model, "no_candidate"));
            return;
        }

        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        long deadline = System.nanoTime() + (long) d.getTotalBudgetMs() * 1_000_000;
        // 候选数 < 配置值时钳到候选数，避免单候选时 ranked.get(i) 越界 500
        int maxAttempts = Math.min(Math.max(1, ranked.size()), Math.max(d.getMaxAttempts(), 1));
        maxAttempts = Math.min(maxAttempts, 6);

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (System.nanoTime() > deadline) break;
            PolicyService.Candidate c = ranked.get(attempt).candidate;
            policyService.tryEnterHalfOpen(c.providerId(), c.model());
            policyService.acquireInflight(c.providerId(), c.model());
            policyService.recordChannelUsed(c.providerId(), c.channelId());
            ForwardEngine.ForwardResult r;
            try {
                r = exec.apply(req, c);
            } finally {
                policyService.releaseInflight(c.providerId(), c.model());
            }
            if (r.kind == ForwardEngine.ForwardResult.Kind.FAILED) {
                policyService.recordFailure(c.providerId(), c.model(), c.channelId(), r.status, r.error, client);
                statsService.logRequest(model, c.providerId(), c.model(), null, null,
                        null, null, "FAILED", attempt + 1, null, r.error, client, null, null, r.status, null, c.channelId());
                log.warn("{}转发失败 attempt={} client={} provider={}/{} status={} err={}",
                        what, attempt + 1, client, c.providerId(), c.model(), r.status, r.error);
                continue;
            }
            // SUCCESS：原样透传上游 JSON，usage 记账进限流学习窗口
            Map<String, Object> parsed = null;
            try { parsed = Json.parseObject(r.body); } catch (Exception ignore) { }
            Map<String, Object> usage = parsed == null ? null : Json.asMap(parsed.get("usage"));
            Long in = usage == null ? null : Json.longVal(usage.get("prompt_tokens"));
            Long out = usage == null ? null : Json.longVal(usage.get("completion_tokens"));
            policyService.recordSuccess(c.providerId(), c.model(), c.channelId(),
                    r.ttftMs / 1_000_000, in, out, false, client, r.ttftMs / 1_000_000);
            statsService.logRequest(model, c.providerId(), c.model(),
                    (int) (r.ttftMs / 1_000_000), (int) (r.ttftMs / 1_000_000),
                    in, out, "OK", attempt + 1, null, null, client, null, null, null, null, c.channelId());
            response.setStatus(200);
            response.setContentType("application/json");
            response.setHeader("X-Gateway-Channel", c.providerId() + "/" + c.channelId());
            response.setHeader("X-Gateway-Attempt", String.valueOf(attempt + 1));
            writeUtf8(response, r.body == null ? "{}" : r.body);
            return;
        }
        writeJson(response, openAiError(504, "gateway_error", what + "全部候选失败: " + model, "all_attempts_failed"));
    }

    /** OpenAI Embeddings 端点透传：向 {baseUrl}/embeddings 整包转发，非流式 JSON 原样返回。 */
    @PostMapping("/v1/embeddings")
    public void embeddings(@RequestHeader(value = "Authorization", required = false) String auth,
                           @RequestHeader(value = "x-api-key", required = false) String apiKeyHeader,
                           @RequestHeader(value = "User-Agent", required = false) String userAgent,
                           @RequestBody String body,
                           HttpServletResponse response) throws IOException {
        String client = ClientDetector.detect(userAgent);
        com.gw.stats.RequestTrace.start(response);
        ResponseEntity<?> authErr = authenticate(auth, apiKeyHeader);
        if (authErr != null) {
            logRejected("-", client, null, "unauthorized 401");
            writeJson(response, authErr); return;
        }

        Map<String, Object> req;
        try {
            req = Json.parseObject(body);
        } catch (Exception e) {
            logRejected("-", client, null, "bad_request: 请求体不是合法 JSON");
            writeJson(response, openAiError(400, "invalid_request_error", "请求体不是合法 JSON: " + e.getMessage(), "bad_request"));
            return;
        }
        String model = Json.str(req.get("model"));
        if (model == null || model.isBlank()) {
            logRejected("-", client, null, "bad_request: model 字段缺失");
            writeJson(response, openAiError(400, "invalid_request_error", "model 字段缺失", "bad_request"));
            return;
        }
        forwardRawEndpoint(req, model, client, "embedding",
                (r, c) -> forwardEngine.executeEmbeddings(r, c, System.nanoTime() + 300_000_000_000L), response);
    }

    /** 视频生成透传：{baseUrl}/videos/generations 整包转发（长超时，普遍>60s）。 */
    @PostMapping("/v1/videos/generations")
    public void videos(@RequestHeader(value = "Authorization", required = false) String auth,
                       @RequestHeader(value = "x-api-key", required = false) String apiKeyHeader,
                       @RequestHeader(value = "User-Agent", required = false) String userAgent,
                       @RequestBody String body,
                       HttpServletResponse response) throws IOException {
        String client = ClientDetector.detect(userAgent);
        com.gw.stats.RequestTrace.start(response);
        ResponseEntity<?> authErr = authenticate(auth, apiKeyHeader);
        if (authErr != null) {
            logRejected("-", client, null, "unauthorized 401");
            writeJson(response, authErr); return;
        }

        Map<String, Object> req;
        try {
            req = Json.parseObject(body);
        } catch (Exception e) {
            logRejected("-", client, null, "bad_request: 请求体不是合法 JSON");
            writeJson(response, openAiError(400, "invalid_request_error", "请求体不是合法 JSON: " + e.getMessage(), "bad_request"));
            return;
        }
        String model = Json.str(req.get("model"));
        if (model == null || model.isBlank()) {
            logRejected("-", client, null, "bad_request: model 字段缺失");
            writeJson(response, openAiError(400, "invalid_request_error", "model 字段缺失", "bad_request"));
            return;
        }
        forwardRawEndpoint(req, model, client, "视频生成",
                (r, c) -> forwardEngine.executeVideos(r, c, System.nanoTime() + 600_000_000_000L), response);
    }

    /** 网关侧直接拒绝（未转发到任何上游）落 request_log：REJECTED。
     *  以前 401/400/404/503 只回给客户端不留痕，统计里"工具没拿到数据"完全不可见。 */
    private void logRejected(String model, String client, String profile, String reason) {
        // 网关侧拒绝：从未选路，没有 Key 归属（channelId 落 NULL）
        statsService.logRequest(model == null || model.isBlank() ? "-" : model, null, null,
                null, null, null, null, "REJECTED", 0, profile, reason, client,
                null, null, rejectStatus(reason), null, null);
    }

    /** 从拒绝原因串推出网关侧 HTTP 状态码（无上游参与）；无法识别返回 null。
     *  数字码优先（Anthropic 的 "no_candidate 529" 应按 529 记，不被关键词 no_candidate→503 抢先）。 */
    static Integer rejectStatus(String reason) {
        if (reason == null) return null;
        if (reason.contains("401")) return 401;
        if (reason.contains("404")) return 404;
        if (reason.contains("503")) return 503;
        if (reason.contains("529")) return 529;
        if (reason.contains("400")) return 400;
        if (reason.contains("unauthorized")) return 401;
        if (reason.contains("model_not_found")) return 404;
        if (reason.contains("no_candidate")) return 503;
        if (reason.contains("bad_request")) return 400;
        return null;
    }

    /** 鉴权：Bearer 或 x-api-key（Anthropic 客户端风格） */
    private ResponseEntity<?> authenticate(String auth, String apiKeyHeader) {
        String key = null;
        if (auth != null && auth.startsWith("Bearer ")) key = auth.substring(7).trim();
        else if (apiKeyHeader != null && !apiKeyHeader.isBlank()) key = apiKeyHeader.trim();
        if (key == null) {
            return openAiError(401, "invalid_request_error", "缺少 Authorization: Bearer <gateway-key>", "unauthorized");
        }
        String hash = ConfigService.sha256(key);
        if (!configService.snapshot().gatewayKeyHashes.containsKey(hash)) {
            return openAiError(401, "invalid_request_error", "无效的 gateway-key", "unauthorized");
        }
        return null;
    }

    private void writeJson(HttpServletResponse response, ResponseEntity<?> re) throws IOException {
        response.setStatus(re.getStatusCode().value());
        response.setContentType("application/json");
        Object body = re.getBody();
        writeUtf8(response, body == null ? "{}" : Json.write(body));
    }

    /** 费用档策略的"无候选"提示：free-only/paid-only 过滤空 = 用户明令的"允许失败"，不是故障 */
    static String tierHint(String strategy) {
        return "free-only".equals(strategy) ? "（当前策略只走免费模型，无免费候选可用——按你的要求不付费兜底，允许失败）"
                : "paid-only".equals(strategy) ? "（当前策略只走付费模型，无付费候选可用）"
                : "（熔断/冷却/禁用/预算耗尽）";
    }

    static ResponseEntity<Map<String, Object>> openAiError(int status, String type, String message, String code) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("message", message);
        err.put("type", type);
        err.put("code", code);
        return ResponseEntity.status(status).body(Map.of("error", err));
    }

    /** UTF-8 写出（Writer 默认 ISO-8859-1 会把中文写成?，统一走字节流） */
    private void writeUtf8(HttpServletResponse response, String text) throws IOException {
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.getOutputStream().write(text.getBytes(StandardCharsets.UTF_8));
    }
}