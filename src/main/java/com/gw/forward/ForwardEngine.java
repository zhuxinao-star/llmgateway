package com.gw.forward;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.policy.PolicyService;
import com.gw.policy.UpstreamErrorClassifier;
import com.gw.provider.AdaptorRegistry;
import com.gw.provider.adaptor.Json;
import com.gw.provider.adaptor.ProtocolAdaptor;
import com.gw.util.ProviderClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 转发引擎：attempt 循环 + 三层超时预算 + SSE 流式透传 + 心跳看门狗。
 *
 * 铁律：仅首 token 之前失败才允许换渠道；已开始输出不切换不误伤。
 */
@Service
public class ForwardEngine {

    private static final Logger log = LoggerFactory.getLogger(ForwardEngine.class);

    private final ConfigService configService;
    private final PolicyService policyService;
    private final AdaptorRegistry adaptorRegistry;
    private final ProviderClients providerClients;

    /** 非流式 body 阻塞读放虚拟线程任务里跑，主线程用 Future.get(剩余预算) 实现精确超时 */
    private final ExecutorService bodyReads = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("gw-body-", 0).factory());

    public ForwardEngine(ConfigService configService, PolicyService policyService,
                         AdaptorRegistry adaptorRegistry, ProviderClients providerClients) {
        this.configService = configService;
        this.policyService = policyService;
        this.adaptorRegistry = adaptorRegistry;
        this.providerClients = providerClients;
    }

    private HttpClient clientFor(GatewayConfig.Provider p) {
        return providerClients.forProvider(p,
                configService.snapshot().config.getDefaults().getConnectTimeoutMs());
    }

    /** 转发结果：调用方拿到后决定写流或整体返回 */
    public static class ForwardResult {
        public enum Kind { SUCCESS, STREAM, FAILED }
        public final Kind kind;
        public final int status;              // FAILED 时为上游状态码或 0（超时/网络）
        public final String body;             // FAILED 时的错误体
        public final Map<String, Object> parsedBody;  // SUCCESS（非流式聚合结果，OpenAI 格式）
        public final BufferedReader streamReader;    // STREAM：上游行流读取器
        public final ProtocolAdaptor adaptor;        // STREAM：继续转换事件用
        public final String firstLine;              // STREAM：execute 内已读出的首个数据行（对冲用）
        public final String providerId, model, channelId;
        public final long ttftMs;
        public final String error;            // 失败原因描述
        public final boolean timeout;         // 是否网关主动超时掐断（快速失败不占 attempt）
        public final long retryAfterSeconds;   // 上游 429/503 返回的 Retry-After（-1=无）

        ForwardResult(Kind kind, int status, String body, Map<String, Object> parsed,
                      BufferedReader reader, ProtocolAdaptor adaptor, String firstLine,
                      String providerId, String model, String channelId,
                      long ttftMs, String error, boolean timeout, long retryAfterSeconds) {
            this.kind = kind;
            this.status = status;
            this.body = body;
            this.parsedBody = parsed;
            this.streamReader = reader;
            this.adaptor = adaptor;
            this.firstLine = firstLine;
            this.providerId = providerId;
            this.model = model;
            this.channelId = channelId;
            this.ttftMs = ttftMs;
            this.error = error;
            this.timeout = timeout;
            this.retryAfterSeconds = retryAfterSeconds;
        }
    }

    /**
     * 单候选执行一次转发。
     * @param openAiRequest OpenAI 格式请求体
     * @param candidate 候选
     * @param stream 请求是否流式
     * @param deadlineNanos 总预算截止
     */
    public ForwardResult execute(Map<String, Object> openAiRequest, PolicyService.Candidate candidate,
                                 boolean stream, long deadlineNanos) {
        return execute(openAiRequest, candidate, stream, deadlineNanos, 0);
    }

    /**
     * 带自定义 HTTP 超时的转发（0=用配置默认 channelBudgetMs）。
     * 上下文压缩的调用输入与原请求同量级（100K+），默认 30s 预算必然超时——
     * 压缩专用 240s；聊天转发不受影响仍走默认。
     */
    public ForwardResult execute(Map<String, Object> openAiRequest, PolicyService.Candidate candidate,
                                 boolean stream, long deadlineNanos, int channelBudgetMsOverride) {
        return execute(openAiRequest, candidate, stream, deadlineNanos, channelBudgetMsOverride, false);
    }

    /**
     * @param forceDisableThinking 撞上"该家默认开思考、要求回传 reasoning_content"时的自愈重试：
     *   对本次未列入实测名单的渠道也注通用关思考开关，对同一候选原样重发一次。
     */
    private ForwardResult execute(Map<String, Object> openAiRequest, PolicyService.Candidate candidate,
                                  boolean stream, long deadlineNanos, int channelBudgetMsOverride,
                                  boolean forceDisableThinking) {
        ConfigService.Snapshot snap = configService.snapshot();
        GatewayConfig.Defaults d = snap.config.getDefaults();
        GatewayConfig.Provider p = snap.providersById.get(candidate.providerId());
        if (p == null) return failed(0, "provider 配置缺失", false);
        GatewayConfig.Channel ch = null;
        for (GatewayConfig.Channel c : p.getChannels()) if (c.getId().equals(candidate.channelId())) ch = c;
        if (ch == null) return failed(0, "channel 配置缺失", false);

        ProtocolAdaptor adaptor = adaptorRegistry.create(p.getProtocol());

        // 真实模型名：routing-override.alias 优先
        ConfigService.Snapshot snapshot = configService.snapshot();
        GatewayConfig.RoutingOverride ro = snapshot.overrideOf(candidate.providerId(), candidate.model());
        String realModel = ro != null && ro.getAlias() != null && !ro.getAlias().isBlank() ? ro.getAlias() : candidate.model();

        // 请求转换
        Map<String, Object> nativeReq = adaptor.transformRequest(openAiRequest, realModel);
        if (stream) {
            nativeReq.put("stream", true);
            // OpenAI 流式默认省略 usage。工具侧（DSH/zcode/codex 等）发流式请求时若不带
            // stream_options.include_usage，上游（AMD/MiniCPM 等）就不回 usage，网关统计记成
            // 0+0、且部分客户端因缺 finish_reason/usage 报 "Stream ended without finish_reason"。
            // 网关统一补上（用户已显式指定则尊重不覆盖）——非流式天然带 usage，不受影响。
            if ("openai".equals(p.getProtocol()) && !nativeReq.containsKey("stream_options")) {
                nativeReq.put("stream_options", Map.of("include_usage", true));
            }
        }
        // 消息规范化仅适用于 OpenAI 出站协议（Anthropic 出站的 system 是顶层字段，不动 messages）
        if ("openai".equals(p.getProtocol())) normalizeMessages(nativeReq);
        // 推理模型默认开思考：小 max_tokens 的请求（Agent 工具常见）token 全烧在思考上、
        // content 为空 → 客户端解析空回复报错。按渠道注入对应的关思考参数（实测各家开关不同）：
        //   sensenova / deepseek 官方：thinking:{"type":"disabled"}
        //   tokenrouter（vLLM 风格）：chat_template_kwargs:{"thinking":false}
        //   amd：不接受参数（注入反而 500），其 qwen3.8 默认即不思考——不注入
        // 用户显式配置了任一开关则尊重不覆盖。
        boolean clientSetSwitch = hasThinkingSwitch(nativeReq);
        boolean disabledThinking = injectThinkingDisable(p, nativeReq, forceDisableThinking);
        String url = p.getBaseUrl().replaceAll("/+$", "") + "/chat/completions";
        if ("anthropic".equals(p.getProtocol())) url = p.getBaseUrl().replaceAll("/+$", "") + "/messages";
        int budgetMs = channelBudgetMsOverride > 0 ? channelBudgetMsOverride : d.getChannelBudgetMs();
        // 响应头超时（三层兜底第一层）：只对 stream=true 生效——流式上游秒发响应头，5s 不发即挂死，
        // 快速判死换家；非流式多数上游"算完才回头"，响应头≈整个计算完成，若也套 5s 就变成总时限
        // （实测把秒回模型的 8-token 探测与压缩摘要调用成片误杀、压垮压缩链路），故非流式沿用通道预算。
        int headerMs = stream && d.getResponseHeaderTimeoutMs() > 0
                ? Math.min(budgetMs, d.getResponseHeaderTimeoutMs()) : budgetMs;
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(headerMs))
                .POST(HttpRequest.BodyPublishers.ofString(Json.write(nativeReq), StandardCharsets.UTF_8));
        for (Map.Entry<String, String> h : adaptor.upstreamHeaders().entrySet()) rb.header(h.getKey(), h.getValue());
        if ("anthropic".equals(p.getProtocol())) rb.header("x-api-key", ch.getApiKey());
        else rb.header("Authorization", "Bearer " + ch.getApiKey());

        long start = System.nanoTime();
        HttpClient client = clientFor(p);
        HttpResponse<InputStream> resp;
        try {
            resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException e) {
            return failed(0, "TCP 连接超时", true);
        } catch (java.net.http.HttpTimeoutException e) {
            return failed(0, "响应头超时(>" + headerMs + "ms)", true);
        } catch (IOException e) {
            return failed(0, "网络错误: " + e.getMessage(), false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failed(0, "中断", false);
        }

        int status = resp.statusCode();
        if (status != 200) {
            String errBody = readAll(resp.body());
            long retryAfter = parseRetryAfterSeconds(resp.headers(), errBody);
            ForwardResult failed = new ForwardResult(ForwardResult.Kind.FAILED, status, errBody, null, null, null, null,
                    candidate.providerId(), candidate.model(), candidate.channelId(), 0, errBody, false, retryAfter);
            // 自愈：该渠道默认开思考，而历史里的 assistant 工具轮没带回 reasoning_content
            // （客户端不发思考内容 / 上一轮由别家服务）→ 关思考对同一候选原样重发一次。
            // 客户端自己指定了思考开关时不越权覆盖；本次已经关过思考还报同样的错就不空转。
            if (status == 400 && !disabledThinking && !clientSetSwitch
                    && UpstreamErrorClassifier.classify(status, errBody)
                        == UpstreamErrorClassifier.ErrorClass.REASONING_PASSBACK) {
                log.warn("[思考回传] {}/{} 要求历史回传 reasoning_content，已关思考重发一次",
                        candidate.providerId(), candidate.model());
                ForwardResult retry = execute(openAiRequest, candidate, stream, deadlineNanos,
                        channelBudgetMsOverride, true);
                if (retry.kind != ForwardResult.Kind.FAILED) return retry;
                // 重发仍失败：回吐原始 400（那才是稳定可解释的那一句），不掩盖
            }
            return failed;
        }

        // 200：读流或整包
        BufferedReader reader = new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8), 8192);
        if (stream) {
            // 在 TTFT 预算内等首个数据行：拿到即视为"已开始输出"（不再切换渠道）；
            // 超时返回 timeout FAILED —— 调用方可立即换渠道重试（而非掐死整单）。
            long ttftDeadlineMs = System.currentTimeMillis() + d.getTtftTimeoutMs();
            String firstLine = null;
            try {
                // readLine() 阻塞无法中断：用剩余预算轮询 ready()，超时即放弃
                while (firstLine == null) {
                    long remain = ttftDeadlineMs - System.currentTimeMillis();
                    if (remain <= 0) break;
                    if (reader.ready()) {
                        String l = reader.readLine();
                        if (l == null) {
                            // 上游直接 EOF：视为流式失败（非超时）
                            return failed(0, "上游提前关闭流", false);
                        }
                        // 真首字判定：只有产出实际内容（content/tool_calls）才算首字。
                        // 空行/keep-alive 注释行/role-only 空 delta 事件都只是"连接活着"，
                        // 不算首字——否则慢模型靠空事件撑过 TTFT 检查、霸占并发位无法 failover。
                        if (adaptor.isRealFirstToken(l)) firstLine = l;
                        // 其余行继续等
                    } else {
                        Thread.sleep(Math.min(25, remain));
                    }
                }
            } catch (IOException e) {
                closeQuietly(reader);
                return failed(0, "网络错误: " + e.getMessage(), false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closeQuietly(reader);
                return failed(0, "中断", false);
            }
            if (firstLine == null) {
                closeQuietly(reader);
                return new ForwardResult(ForwardResult.Kind.FAILED, 0, null, null, null, null, null,
                        candidate.providerId(), candidate.model(), candidate.channelId(), 0,
                        "首字超时(>" + d.getTtftTimeoutMs() + "ms)", true, -1);
            }
            long ttft = System.nanoTime() - start;
            return new ForwardResult(ForwardResult.Kind.STREAM, 200, null, null, reader, adaptor, firstLine,
                    candidate.providerId(), candidate.model(), candidate.channelId(), ttft, null, false, -1);
        } else {
            // 非流式：完整读取。HttpRequest.timeout 只覆盖到响应头到达（ofInputStream），
            // 头后 body 停滞要靠预算兜底。用虚拟线程执行阻塞读 + Future.get(剩余预算)：
            // ready() 轮询不可靠（body 无换行且读完后 ready() 谎报 false → 完整响应被误判超时），
            // 超时后 close 流可让阻塞中的 read 立即抛 IOException 退出，不留挂死线程。
            long remainMs = (deadlineNanosSafe(deadlineNanos, start, budgetMs) - System.nanoTime()) / 1_000_000L;
            Future<String> bodyF = bodyReads.submit(() -> readAll(reader));
            String body;
            try {
                body = bodyF.get(Math.max(1, remainMs), TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                bodyF.cancel(true);
                closeQuietly(reader);
                return new ForwardResult(ForwardResult.Kind.FAILED, 0, null, null, null, null, null,
                        candidate.providerId(), candidate.model(), candidate.channelId(), 0,
                        "响应体读取超时(>" + budgetMs + "ms)", true, -1);
            } catch (InterruptedException ie) {
                bodyF.cancel(true);
                Thread.currentThread().interrupt();
                closeQuietly(reader);
                return failed(0, "中断", false);
            } catch (Exception e) {
                closeQuietly(reader);
                return failed(0, "读取/解析失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), false);
            }
            try {
                Map<String, Object> parsed = Json.parseObject(body);
                Map<String, Object> openAiResp = adaptor.transformResponse(parsed);
                return new ForwardResult(ForwardResult.Kind.SUCCESS, 200, null, openAiResp, null, adaptor, null,
                        candidate.providerId(), candidate.model(), candidate.channelId(),
                        System.nanoTime() - start, null, false, -1);
            } catch (Exception e) {
                return failed(0, "响应解析失败: " + e.getMessage(), false);
            }
        }
    }

    /** body 读取截止：min(总预算, 发起到 channelBudgetMs) */
    private long deadlineNanosSafe(long deadlineNanos, long startNanos, int budgetMs) {
        long channelDeadline = startNanos + (long) budgetMs * 1_000_000L;
        return Math.min(deadlineNanos == 0 ? Long.MAX_VALUE : deadlineNanos, channelDeadline);
    }

    /** 非流式整包读结果：body 与 error 互斥；timeout 供上层决定是否可重试 */
    private record BodyRead(String body, String error, boolean timeout) {}

    /**
     * 预算内读完非流式响应体（修 #4）：HttpRequest.timeout 只覆盖到响应头，头后 body 停滞
     * 或读中断（IOException）此前被 readAll 吞成空串记 SUCCESS——这里超时/读失败一律 FAILED，
     * 让上层 failover 换候选，而不是把 "{}" 回写给客户端。
     */
    private BodyRead readBodyWithinBudget(HttpResponse<InputStream> resp, long deadlineNanos, long startNanos, int budgetMs) {
        java.io.InputStream in = resp.body();
        long remainMs = (deadlineNanosSafe(deadlineNanos, startNanos, budgetMs) - System.nanoTime()) / 1_000_000L;
        Future<String> f = bodyReads.submit(() -> new String(in.readAllBytes(), StandardCharsets.UTF_8));
        try {
            return new BodyRead(f.get(Math.max(1, remainMs), TimeUnit.MILLISECONDS), null, false);
        } catch (TimeoutException te) {
            f.cancel(true);
            closeQuietly(in);
            return new BodyRead(null, "响应体读取超时(>" + budgetMs + "ms)", true);
        } catch (InterruptedException ie) {
            f.cancel(true);
            Thread.currentThread().interrupt();
            closeQuietly(in);
            return new BodyRead(null, "中断", false);
        } catch (Exception e) {
            closeQuietly(in);
            return new BodyRead(null, "读取失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), false);
        }
    }

    /**
     * 图像生成转发（/v1/images/generations）：非流式整包透传。
     * 与聊天共用候选/Key 机制；请求体原样转发（OpenAI Images 格式），
     * 成功返回 SUCCESS 且 body 为上游原始 JSON 字符串（不解析，保留 url/b64_json）。
     */
    public ForwardResult executeImages(Map<String, Object> imagesRequest, PolicyService.Candidate candidate,
                                       long deadlineNanos) {
        ConfigService.Snapshot snap = configService.snapshot();
        GatewayConfig.Defaults d = snap.config.getDefaults();
        GatewayConfig.Provider p = snap.providersById.get(candidate.providerId());
        if (p == null) return failed(0, "provider 配置缺失", false);
        GatewayConfig.Channel ch = null;
        for (GatewayConfig.Channel c : p.getChannels()) if (c.getId().equals(candidate.channelId())) ch = c;
        if (ch == null) return failed(0, "channel 配置缺失", false);

        // 真实模型名：routing-override.alias 优先
        GatewayConfig.RoutingOverride ro = snap.overrideOf(candidate.providerId(), candidate.model());
        String realModel = ro != null && ro.getAlias() != null && !ro.getAlias().isBlank() ? ro.getAlias() : candidate.model();

        Map<String, Object> nativeReq = new LinkedHashMap<>(imagesRequest);
        nativeReq.put("model", realModel);

        String url = p.getBaseUrl().replaceAll("/+$", "") + "/images/generations";
        int budgetMs = Math.max(d.getChannelBudgetMs(), 60_000);  // 图像生成普遍 >30s，至少给 60s
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(budgetMs))
                .POST(HttpRequest.BodyPublishers.ofString(Json.write(nativeReq), StandardCharsets.UTF_8));
        rb.header("Authorization", "Bearer " + ch.getApiKey());
        rb.header("Content-Type", "application/json");

        long start = System.nanoTime();
        HttpClient client = clientFor(p);
        HttpResponse<InputStream> resp;
        try {
            resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException e) {
            return failed(0, "TCP 连接超时", true);
        } catch (IOException e) {
            return failed(0, "网络错误: " + e.getMessage(), false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failed(0, "中断", false);
        }
        int status = resp.statusCode();
        // 预算内读 body：超时/中途断流记 FAILED 交给 failover（修复吞成空 body 记 SUCCESS）
        BodyRead br = readBodyWithinBudget(resp, deadlineNanos, start, budgetMs);
        if (br.error() != null) {
            return new ForwardResult(ForwardResult.Kind.FAILED, status, br.error(), null, null, null, null,
                    candidate.providerId(), candidate.model(), candidate.channelId(), 0, br.error(), br.timeout(), -1);
        }
        String body = br.body();
        if (status != 200) {
            long retryAfter = parseRetryAfterSeconds(resp.headers(), body);
            return new ForwardResult(ForwardResult.Kind.FAILED, status, body, null, null, null, null,
                    candidate.providerId(), candidate.model(), candidate.channelId(), 0, body, false, retryAfter);
        }
        if (body == null || body.isBlank()) {
            // 200 空 body=上游断流，原样回写等于客户端拿"没有结论的结论"，宁可 failover
            return new ForwardResult(ForwardResult.Kind.FAILED, 200, "上游 200 但响应体为空", null, null, null, null,
                    candidate.providerId(), candidate.model(), candidate.channelId(), 0, "上游 200 但响应体为空", false, -1);
        }
        return new ForwardResult(ForwardResult.Kind.SUCCESS, 200, body, null, null, null, null,
                candidate.providerId(), candidate.model(), candidate.channelId(),
                System.nanoTime() - start, null, false, -1);
    }

    /**
     * 通用非流式透传（embedding / 视频等）：同 images 一样原样转发到 {baseUrl}{endpointPath}，
     * 请求体保留（只替换 model 为真实模型名），非流式 JSON 原样返回。
     */
    public ForwardResult executeRaw(String endpointPath, Map<String, Object> request,
                                    PolicyService.Candidate candidate, long deadlineNanos, int minTimeoutMs) {
        ConfigService.Snapshot snap = configService.snapshot();
        GatewayConfig.Defaults d = snap.config.getDefaults();
        GatewayConfig.Provider p = snap.providersById.get(candidate.providerId());
        if (p == null) return failed(0, "provider 配置缺失", false);
        GatewayConfig.Channel ch = null;
        for (GatewayConfig.Channel c : p.getChannels()) if (c.getId().equals(candidate.channelId())) ch = c;
        if (ch == null) return failed(0, "channel 配置缺失", false);

        GatewayConfig.RoutingOverride ro = snap.overrideOf(candidate.providerId(), candidate.model());
        String realModel = ro != null && ro.getAlias() != null && !ro.getAlias().isBlank() ? ro.getAlias() : candidate.model();

        Map<String, Object> nativeReq = new LinkedHashMap<>(request);
        nativeReq.put("model", realModel);

        String url = p.getBaseUrl().replaceAll("/+$", "") + endpointPath;
        int budgetMs = Math.max(d.getChannelBudgetMs(), minTimeoutMs);
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(budgetMs))
                .POST(HttpRequest.BodyPublishers.ofString(Json.write(nativeReq), StandardCharsets.UTF_8));
        rb.header("Authorization", "Bearer " + ch.getApiKey());
        rb.header("Content-Type", "application/json");

        long start = System.nanoTime();
        HttpClient client = clientFor(p);
        HttpResponse<InputStream> resp;
        try {
            resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException e) {
            return failed(0, "TCP 连接超时", true);
        } catch (IOException e) {
            return failed(0, "网络错误: " + e.getMessage(), false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failed(0, "中断", false);
        }
        int status = resp.statusCode();
        // 预算内读 body：超时/中途断流记 FAILED 交给 failover（修复吞成空 body 记 SUCCESS）
        BodyRead br = readBodyWithinBudget(resp, deadlineNanos, start, budgetMs);
        if (br.error() != null) {
            return new ForwardResult(ForwardResult.Kind.FAILED, status, br.error(), null, null, null, null,
                    candidate.providerId(), candidate.model(), candidate.channelId(), 0, br.error(), br.timeout(), -1);
        }
        String body = br.body();
        if (status != 200) {
            long retryAfter = parseRetryAfterSeconds(resp.headers(), body);
            return new ForwardResult(ForwardResult.Kind.FAILED, status, body, null, null, null, null,
                    candidate.providerId(), candidate.model(), candidate.channelId(), 0, body, false, retryAfter);
        }
        if (body == null || body.isBlank()) {
            // 200 空 body=上游断流，原样回写等于客户端拿"没有结论的结论"，宁可 failover
            return new ForwardResult(ForwardResult.Kind.FAILED, 200, "上游 200 但响应体为空", null, null, null, null,
                    candidate.providerId(), candidate.model(), candidate.channelId(), 0, "上游 200 但响应体为空", false, -1);
        }
        return new ForwardResult(ForwardResult.Kind.SUCCESS, 200, body, null, null, null, null,
                candidate.providerId(), candidate.model(), candidate.channelId(),
                System.nanoTime() - start, null, false, -1);
    }

    /** embedding 透传：{baseUrl}/embeddings，非流式整包返回 */
    public ForwardResult executeEmbeddings(Map<String, Object> req, PolicyService.Candidate c, long deadlineNanos) {
        return executeRaw("/embeddings", req, c, deadlineNanos, 30_000);
    }

    /** 视频生成透传：{baseUrl}/videos/generations（部分上游为 /videos），非流式整包返回 */
    public ForwardResult executeVideos(Map<String, Object> req, PolicyService.Candidate c, long deadlineNanos) {
        return executeRaw("/videos/generations", req, c, deadlineNanos, 120_000);
    }

    private String readAll(java.io.InputStream in) {
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    private String readAll(BufferedReader r) {
        try (r) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = r.readLine()) != null) sb.append(line).append('\n');
            return sb.toString();
        } catch (IOException e) {
            return "";
        }
    }

    private ForwardResult failed(int status, String error, boolean timeout) {
        return new ForwardResult(ForwardResult.Kind.FAILED, status, error, null, null, null, null,
                null, null, null, 0, error, timeout, -1);
    }

    /**
     * 按渠道注入"关闭思考"参数（各家实测开关不同）：
     * - sensenova：thinking:{"type":"disabled"} + reasoning_effort:"none"（glm-5.2 拒绝单用 disabled，
     *   报 "invalid thinking type"，两者组合对 deepseek-v4-flash / glm-5.2 均实测有效）
     * - deepseek 官方：thinking:{"type":"disabled"}
     *   （它要求历史 reasoning_content 回传，网关不透传 → 不关思考多轮必 400）
     * - tokenrouter：chat_template_kwargs:{"thinking":false}（vLLM 风格；glm-5.3-free 有时锁思考——
     *   注了也关不掉时靠 max_tokens 预算兜底，参数无害保留）
     * - amd：不接受任一参数（注入直接 500），默认不思考——不注入
     * 名单外的渠道默认不动它：靠"撞墙自愈"（见 execute 的 REASONING_PASSBACK 分支）或该供应商
     * 显式配 thinkingMode: disable。名单是按 provider id 枚举的历史包袱，新渠道不再需要往里加。
     * 用户请求里已带任一思考开关（thinking / chat_template_kwargs / enable_thinking / reasoning_effort）则全部尊重不覆盖。
     */
    /** 请求里是否已带任一思考开关（带了就尊重客户端，网关一律不覆盖）。 */
    private static boolean hasThinkingSwitch(Map<String, Object> req) {
        return req.containsKey("thinking") || req.containsKey("chat_template_kwargs")
                || req.containsKey("enable_thinking") || req.containsKey("reasoning_effort");
    }

    /**
     * @return 网关本次是否已把思考关掉（关掉后同一次请求不会再自我重试）。
     */
    private static boolean injectThinkingDisable(GatewayConfig.Provider p, Map<String, Object> nativeReq,
                                                 boolean forceDisable) {
        if (hasThinkingSwitch(nativeReq)) return false;
        switch (p.getId()) {
            case "sensenova" -> {
                nativeReq.put("thinking", Map.of("type", "disabled"));
                nativeReq.put("reasoning_effort", "none");
                return true;
            }
            case "deepseek" -> {
                nativeReq.put("thinking", Map.of("type", "disabled"));
                return true;
            }
            case "tokenrouter" -> {
                nativeReq.put("chat_template_kwargs", Map.of("thinking", false));
                return true;
            }
            default -> {
                // 未列入实测名单的渠道（amd / agnes / openrouter / 新加的转售商…）：
                // 只有"该家确实因默认开思考而 400"（forceDisable，撞墙自愈）或用户在该供应商上
                // 显式配了 thinkingMode: disable，才注 OpenAI 生态最通用的开关。
                // 实测 lingxiongkeji 认 thinking:{"type":"disabled"}（reasoning_tokens 34→0）；
                // amd 不接受任何思考参数（注了直接 500），故默认仍不动它。
                boolean want = forceDisable || "disable".equalsIgnoreCase(p.getThinkingMode());
                if (want && "openai".equals(p.getProtocol())) {
                    nativeReq.put("thinking", Map.of("type", "disabled"));
                    return true;
                }
                return false;
            }
        }
    }

    private static void closeQuietly(BufferedReader r) {
        try { r.close(); } catch (IOException ignore) {}
    }

    private static void closeQuietly(java.io.InputStream in) {
        try { in.close(); } catch (IOException ignore) {}
    }

    /**
     * 消息规范化（上游兼容性）：
     * 1. system 消息归位到开头（部分上游如 AMD 拒绝中间 system）并合并多条；
     * 2. assistant 消息 content=null 时置空字符串（部分上游拒绝 null）；
     * 3. 删除空消息（content 空且无 tool_calls）；
     * 4. tool 消息序合规（OpenAI 严格要求：tool 必须紧跟含 tool_calls 的 assistant 消息）：
     *    - 孤儿 tool 消息（前面没有对应 tool_call_id 的 assistant）转成 user 消息带引用说明；
     *    - assistant(tool_calls) 与其 tool 结果之间插入的其他消息（system/user）移到 tool 组之后。
     */
    private static void normalizeMessages(Map<String, Object> nativeReq) {
        List<Object> messages = com.gw.provider.adaptor.Json.asList(nativeReq.get("messages"));
        if (messages == null || messages.isEmpty()) return;
        List<Object> out = new ArrayList<>();
        StringBuilder systemText = null;
        for (Object o : messages) {
            Map<String, Object> m = com.gw.provider.adaptor.Json.asMap(o);
            if (m == null) continue;
            String role = com.gw.provider.adaptor.Json.str(m.get("role"));
            if ("system".equals(role)) {
                Object c = m.get("content");
                String text = c instanceof String s ? s : (c == null ? "" : com.gw.provider.adaptor.Json.write(c));
                if (!text.isEmpty()) {
                    if (systemText == null) systemText = new StringBuilder();
                    else systemText.append('\n');
                    systemText.append(text);
                }
                continue;
            }
            Map<String, Object> copy = new LinkedHashMap<>(m);
            Object content = copy.get("content");
            boolean hasToolCalls = copy.get("tool_calls") != null;
            boolean emptyContent = content == null || (content instanceof String s && s.isEmpty());
            if ("assistant".equals(role) && emptyContent && hasToolCalls) {
                copy.put("content", "");  // null content + tool_calls 的上游兼容
            }
            if (emptyContent && !hasToolCalls) continue;  // 空消息删除
            // OpenAI tool 消息 content 可能为 null
            if ("tool".equals(role) && content == null) copy.put("content", "");
            out.add(copy);
        }

        // ---- tool 序列重排：assistant(tool_calls) 后面必须紧跟它的全部 tool 消息 ----
        List<Object> fixed = new ArrayList<>();
        // 1) 收集每个 assistant tool_call_id 的位置；2) 把每条 tool 消息挂到对应 assistant 之后
        Map<String, List<Object>> toolsByPendingCall = new LinkedHashMap<>();  // 待补的 tool 消息（按 tool_call_id）
        List<String> pendingToolCallIds = new ArrayList<>();                   // 当前 assistant(tool_calls) 声明的 id
        for (Object o : out) {
            Map<String, Object> m = (Map<String, Object>) o;
            String role = com.gw.provider.adaptor.Json.str(m.get("role"));
            if ("assistant".equals(role) && m.get("tool_calls") != null) {
                // 新的 assistant(tool_calls)：先结算上一组（缺的补空 tool 消息）
                flushPendingTools(fixed, toolsByPendingCall, pendingToolCallIds);
                fixed.add(m);
                pendingToolCallIds = new ArrayList<>();
                for (Object tc : com.gw.provider.adaptor.Json.asList(m.get("tool_calls"))) {
                    Map<String, Object> t = com.gw.provider.adaptor.Json.asMap(tc);
                    if (t != null && t.get("id") != null) pendingToolCallIds.add(com.gw.provider.adaptor.Json.str(t.get("id")));
                }
                toolsByPendingCall = new LinkedHashMap<>();
                continue;
            }
            if ("tool".equals(role)) {
                String callId = com.gw.provider.adaptor.Json.str(m.get("tool_call_id"));
                if (callId != null && pendingToolCallIds.contains(callId)) {
                    // 合法：紧跟所属 assistant → 直接挂上
                    toolsByPendingCall.computeIfAbsent(callId, k -> new ArrayList<>()).add(m);
                    continue;
                }
                // 孤儿 tool（对应 assistant 不在或 tool_calls 已结算）：
                // 转成 user 消息保内容（直接删可能丢工具结果，Claude Code 对话里很重要）
                Map<String, Object> asUser = new LinkedHashMap<>();
                asUser.put("role", "user");
                asUser.put("content", "[tool result for " + (callId == null ? "unknown" : callId) + "] "
                        + (m.get("content") == null ? "" : m.get("content")));
                fixed.add(asUser);
                continue;
            }
            // user/assistant 普通消息：只要还有未结算的 tool_calls 声明就先结算（缺的补空），
            // 避免 tool 结果迟到时被 user 挡路 → 后到时被误判孤儿
            if (!pendingToolCallIds.isEmpty()) {
                flushPendingTools(fixed, toolsByPendingCall, pendingToolCallIds);
                pendingToolCallIds = new ArrayList<>();
                toolsByPendingCall = new LinkedHashMap<>();
            }
            fixed.add(m);
        }
        // 尾部还有未结算的 tool 组
        flushPendingTools(fixed, toolsByPendingCall, pendingToolCallIds);

        if (systemText != null) {
            Map<String, Object> sys = new LinkedHashMap<>();
            sys.put("role", "system");
            sys.put("content", systemText.toString());
            fixed.add(0, sys);
        }
        if (fixed.isEmpty()) {
            fixed.add(Map.of("role", "user", "content", ""));
        }
        nativeReq.put("messages", fixed);
    }

    /** 把挂起的 tool 消息插入消息列表；声明了却缺结果的 tool_call_id 补一条空结果（上游要求每个 id 都有回应） */
    private static void flushPendingTools(List<Object> fixed, Map<String, List<Object>> toolsByPendingCall,
                                          List<String> pendingToolCallIds) {
        if (pendingToolCallIds.isEmpty()) return;
        for (String id : pendingToolCallIds) {
            List<Object> ts = toolsByPendingCall.get(id);
            if (ts != null && !ts.isEmpty()) {
                fixed.addAll(ts);
            } else {
                Map<String, Object> stub = new LinkedHashMap<>();
                stub.put("role", "tool");
                stub.put("tool_call_id", id);
                stub.put("content", "");
                fixed.add(stub);
            }
        }
    }

    /** 从上游响应提取 Retry-After 秒数（429/503 处理）。优先级：header(整数/浮点/HTTP-date) → JSON body 字段。
     * 返回 -1 = 无精确值（用默认冷却）。 */
    private static final long MAX_RETRY_AFTER_SECONDS = 86400L;   // 单次 Retry-After 上限 24h：防恶意/异常值饱和 long 永久冻结渠道
    public static long parseRetryAfterSeconds(HttpHeaders headers, String body) {
        Optional<String> ra = headers.firstValue("Retry-After").stream().findFirst();
        if (ra.isPresent()) {
            String v = ra.get().trim();
            try {
                double d = Double.parseDouble(v);
                if (Double.isFinite(d)) return clampRetryAfter((long) d);
            } catch (NumberFormatException ignore) {}
            // HTTP-date 格式（如 "Wed, 21 Oct 2015 07:28:00 GMT"）→ 距 now 的秒
            try {
                long until = java.time.ZonedDateTime.parse(v, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME)
                        .toInstant().toEpochMilli();
                long sec = (until - System.currentTimeMillis()) / 1000;
                return clampRetryAfter(sec);
            } catch (Exception ignore) {}
        }
        // JSON body：{ "error": { "retry_after": N } } 或 { "retry_after": N } 或 retry-after
        if (body != null && !body.isBlank()) {
            try {
                Map<String, Object> j = Json.parseObject(body);
                Object err = j.get("error");
                Map<String, Object> em = err instanceof Map ? (Map<String, Object>) err : j;
                for (String k : new String[]{"retry_after", "retry-after"}) {
                    Object o = em.get(k);
                    if (o instanceof Number n) return clampRetryAfter(n.longValue());
                    if (o instanceof String s) {
                        try {
                            double d = Double.parseDouble(s.trim());
                            if (Double.isFinite(d)) return clampRetryAfter((long) d);
                        } catch (NumberFormatException ignore) {}
                    }
                }
            } catch (Exception ignore) {}
        }
        return -1;
    }

    /** 钳到 [1, 24h]：负数/0 记 1 秒，超大值（含 double→long 饱和）封顶一天，防永久冻结 */
    private static long clampRetryAfter(long seconds) {
        if (seconds <= 0) return 1;
        return Math.min(seconds, MAX_RETRY_AFTER_SECONDS);
    }
}
