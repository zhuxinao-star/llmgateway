package com.gw;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock 上游服务器：模拟 OpenAI 兼容 / Anthropic 上游，验证网关全链路。
 *
 * 行为由端口区分：
 *  - 18101 OpenAI 兼容、正常（流式 chunked SSE + 非流式）
 *  - 18102 OpenAI 兼容、永远 429（Retry-After: 2）
 *  - 18103 OpenAI 兼容、TCP 能连但响应头极慢（挂 20s）——验证响应头超时
 *  - 18104 OpenAI 兼容、流式首包前卡 20s——验证 TTFT 超时
 *  - 18105 Anthropic 协议（/v1/messages 流式事件 + /v1/models）
 *  - 18106 OpenAI 兼容、SSE 发 2 个事件后卡死——验证流中心跳超时（长测）
 */
public class MockUpstream {

    public static void main(String[] args) throws Exception {
        startOpenAi(18101, "normal", 0);
        startOpenAi(18102, "429", 0);
        startOpenAi(18103, "slow-header", 0);
        startOpenAi(18104, "slow-first-token", 0);
        startAnthropic(18105);
        startOpenAi(18106, "stream-stall", 0);
        startOpenAi(18107, "keepalive-stall", 0);
        startOpenAi(18108, "stream-die", 0);
        startOpenAi(18109, "stream-garbage-done", 0);
        System.out.println("Mock upstreams ready on 18101-18109");
        Thread.currentThread().join();
    }

    static void startOpenAi(int port, String behavior, long unused) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        AtomicInteger reqCount = new AtomicInteger();

        server.createContext("/v1/models", ex -> {
            byte[] body = """
                {"object":"list","data":[{"id":"mock-model","object":"model"},{"id":"mock-fast","object":"model"}]}
                """.getBytes(StandardCharsets.UTF_8);
            respond(ex, 200, "application/json", body);
        });

        server.createContext("/v1/chat/completions", ex -> {
            int n = reqCount.incrementAndGet();
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.printf("[mock:%d %s] req#%d body=%s%n", port, behavior, n, body.substring(0, Math.min(120, body.length())));
            switch (behavior) {
                case "429" -> {
                    ex.getResponseHeaders().add("Retry-After", "2");
                    respond(ex, 429, "application/json",
                            ("{\"error\":{\"message\":\"rate limit (mock)\",\"type\":\"rate_limit_error\"}}").getBytes(StandardCharsets.UTF_8));
                }
                case "slow-header" -> {
                    try { Thread.sleep(40_000); } catch (InterruptedException ignore) {}  // 超过 30s 渠道预算：网关必须掐断
                    respond(ex, 200, "application/json", "{}".getBytes(StandardCharsets.UTF_8));
                }
                case "slow-first-token" -> {
                    // 先把 200 头发出去，然后首 token 前卡死
                    ex.getResponseHeaders().add("Content-Type", "text/event-stream");
                    ex.sendResponseHeaders(200, 0);
                    try (OutputStream out = ex.getResponseBody()) {
                        Thread.sleep(20_000);  // TTFT 超时（默认 10s）应掐断
                    } catch (InterruptedException ignore) {}
                }
                case "stream-stall" -> {
                    ex.getResponseHeaders().add("Content-Type", "text/event-stream");
                    ex.sendResponseHeaders(200, 0);
                    try (OutputStream out = ex.getResponseBody()) {
                        out.write(("data: " + chunk("Hello ") + "\n\n").getBytes(StandardCharsets.UTF_8));
                        out.write(("data: " + chunk("world ") + "\n\n").getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        Thread.sleep(90_000);  // 心跳超时（默认 30s）应掐断
                    } catch (InterruptedException ignore) {}
                }
                case "keepalive-stall" -> {
                    // 出过一个 token 后开始无限发 SSE 注释行（keep-alive）但永不产内容——
                    // 旧看门狗会被无限续命；新窗口逻辑（注释行不刷新已出 token 后的窗口）应在
                    // firstTokenHeartbeatTimeoutMs 内掐断。注释行之外还穿插空行（SSE 分隔）。
                    ex.getResponseHeaders().add("Content-Type", "text/event-stream");
                    ex.sendResponseHeaders(200, 0);
                    try (OutputStream out = ex.getResponseBody()) {
                        out.write(("data: " + chunk("开始") + "\n\n").getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        for (int i = 0; i < 300; i++) {
                            out.write(": keep-alive comment\n\n".getBytes(StandardCharsets.UTF_8));
                            out.flush();
                            Thread.sleep(1000);
                        }
                    } catch (InterruptedException ignore) {}
                }
                case "stream-die" -> {
                    // 上游中途断流：2 个完整内容 chunk + usage chunk 后，写半截残行（JSON 未闭合、
                    // 无换行）随即关闭连接（无 [DONE]）——模拟 AMD 等免费上游生成中途杀连接的场景。
                    ex.getResponseHeaders().add("Content-Type", "text/event-stream");
                    ex.sendResponseHeaders(200, 0);
                    OutputStream out = ex.getResponseBody();
                    out.write(("data: " + chunk("前半段") + "\n\n").getBytes(StandardCharsets.UTF_8));
                    out.write(("data: " + chunk("后半段") + "\n\n").getBytes(StandardCharsets.UTF_8));
                    out.write(("data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-model\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n")
                            .getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    try { Thread.sleep(150); } catch (InterruptedException ignore) {}
                    out.write(("data: {\"choices\":[{\"delta\":{\"content\":\"断尾\"").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    ex.close();   // 关闭底层连接 = EOF 且无 [DONE]（必须显式 close，HttpServer 不会自动断）
                }
                case "stream-garbage-done" -> {
                    // 复刻 turn 6 实况：内容 + usage 后吐一行未闭合 JSON（垃圾行），随后仍发 [DONE]。
                    // 网关必须丢弃垃圾行、且因流已污染不再向客户端发 [DONE]（按失败记账，客户端干净 EOF）。
                    ex.getResponseHeaders().add("Content-Type", "text/event-stream");
                    ex.sendResponseHeaders(200, 0);
                    try (OutputStream out = ex.getResponseBody()) {
                        out.write(("data: " + chunk("前半段") + "\n\n").getBytes(StandardCharsets.UTF_8));
                        out.write(("data: " + chunk("后半段") + "\n\n").getBytes(StandardCharsets.UTF_8));
                        out.write(("data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-model\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n")
                                .getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        Thread.sleep(150);
                        out.write(("data: {\"choices\":[{\"delta\":{\"content\":\"垃圾行\"\n\n").getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        Thread.sleep(150);
                        out.write("data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    } catch (InterruptedException ignore) {}
                }
                default -> {
                    boolean stream = body.contains("\"stream\":true") || body.contains("\"stream\": true");
                    if (stream) {
                        ex.getResponseHeaders().add("Content-Type", "text/event-stream");
                        ex.sendResponseHeaders(200, 0);
                        try (OutputStream out = ex.getResponseBody()) {
                            for (String w : new String[]{"你好", "，", "这是", "Mock", "流式", "响应", "。"}) {
                                out.write(("data: " + chunk(w) + "\n\n").getBytes(StandardCharsets.UTF_8));
                                out.flush();
                                Thread.sleep(60);
                            }
                            // 最后一个 chunk 带 usage
                            out.write(("data: " + finalChunkWithUsage() + "\n\n").getBytes(StandardCharsets.UTF_8));
                            out.write("data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8));
                            out.flush();
                        } catch (InterruptedException ignore) {}
                    } else {
                        String resp = """
                            {"id":"chatcmpl-mock1","object":"chat.completion","created":1,"model":"mock-model",
                             "choices":[{"index":0,"message":{"role":"assistant","content":"Mock 非流式响应"},"finish_reason":"stop"}],
                             "usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20}}
                            """.strip().replaceAll("\n", "");
                        respond(ex, 200, "application/json", resp.getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        });
        server.start();
    }

    static void startAnthropic(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/v1/models", ex -> respond(ex, 200, "application/json",
                ("{\"data\":[{\"id\":\"claude-mock\"}],\"has_more\":false}").getBytes(StandardCharsets.UTF_8)));
        server.createContext("/v1/messages", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.printf("[mock:%d anthropic] body=%s%n", port, body.substring(0, Math.min(200, body.length())));
            // 校验 anthropic-version 头
            String ver = ex.getRequestHeaders().getFirst("anthropic-version");
            System.out.println("  anthropic-version=" + ver + " x-api-key=" +
                    (ex.getRequestHeaders().getFirst("x-api-key") == null ? "MISSING" : "present"));
            if (body.contains("\"stream\":true") || body.contains("\"stream\": true")) {
                ex.getResponseHeaders().add("Content-Type", "text/event-stream");
                ex.sendResponseHeaders(200, 0);
                try (OutputStream out = ex.getResponseBody()) {
                    StringBuilder events = new StringBuilder();
                    events.append("event: message_start\n").append("data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock\",\"model\":\"claude-mock\",\"usage\":{\"input_tokens\":25}}}\n\n");
                    events.append("event: content_block_start\n").append("data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n");
                    out.write(events.toString().getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    for (String w : new String[]{"Anthropic ", "协议 ", "转换 ", "测试 ", "OK"}) {
                        out.write(("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"" + w + "\"}}\n\n")
                                .getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        Thread.sleep(50);
                    }
                    out.write(("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n").getBytes(StandardCharsets.UTF_8));
                    out.write(("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":18}}\n\n")
                            .getBytes(StandardCharsets.UTF_8));
                    out.write(("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                } catch (InterruptedException ignore) {}
            } else {
                String resp = """
                    {"id":"msg_mock","type":"message","role":"assistant","model":"claude-mock",
                     "content":[{"type":"text","text":"Anthropic 非流式转换 OK"}],
                     "stop_reason":"end_turn","usage":{"input_tokens":30,"output_tokens":22}}
                    """.strip().replaceAll("\n", "");
                respond(ex, 200, "application/json", resp.getBytes(StandardCharsets.UTF_8));
            }
        });
        server.start();
    }

    static String chunk(String content) {
        return "{\"id\":\"chatcmpl-mock1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"mock-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + content + "\"},\"finish_reason\":null}]}";
    }

    static String finalChunkWithUsage() {
        return "{\"id\":\"chatcmpl-mock1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"mock-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":15,\"completion_tokens\":9,\"total_tokens\":24}}";
    }

    static void respond(HttpExchange ex, int status, String contentType, byte[] body) throws IOException {
        try (ex) {
            ex.getResponseHeaders().add("Content-Type", contentType);
            ex.sendResponseHeaders(status, body.length);
            ex.getResponseBody().write(body);
        }
    }
}
