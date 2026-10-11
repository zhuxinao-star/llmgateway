package com.gw.provider.adaptor;

import java.util.*;

/**
 * Anthropic 适配器：OpenAI 格式 ⇄ Claude 原生格式，含流式事件转换。
 *
 * 关键差异处理：
 * - system 是顶层字段而非 messages[0]
 * - max_tokens 必填
 * - tool_calls → content blocks (type=tool_use)
 * - tool 消息 → user role 的 tool_result block
 * - 流式：message_start/content_block_start/content_block_delta/content_block_stop/message_delta/message_stop
 *   → OpenAI chunk；usage 在 message_start 与 message_delta 中
 *   → OpenAI chunk；usage 在 message_start 与 message_delta 中
 */
public class AnthropicAdaptor implements ProtocolAdaptor {

    @Override
    public String protocol() { return "anthropic"; }

    @Override
    public Map<String, Object> transformRequest(Map<String, Object> req, String realModel) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("model", realModel);
        out.put("max_tokens", req.get("max_tokens") == null ? 4096 : req.get("max_tokens"));

        // system: OpenAI messages[0].role=system → 顶层 system
        List<Object> messages = Json.asList(req.get("messages"));
        List<Object> outMessages = new ArrayList<>();
        if (messages != null) {
            for (Object o : messages) {
                Map<String, Object> m = Json.asMap(o);
                if (m == null) continue;
                String role = Json.str(m.get("role"));
                if ("system".equals(role)) {
                    out.put("system", m.get("content"));
                    continue;
                }
                if ("tool".equals(role)) {
                    // OpenAI tool 消息 → user 的 tool_result block
                    String toolCallId = Json.str(m.get("tool_call_id"));
                    Map<String, Object> block = Json.obj("type", "tool_result", "tool_use_id", toolCallId,
                            "content", m.get("content"));
                    outMessages.add(Json.obj("role", "user", "content", Json.arr(block)));
                    continue;
                }
                if ("assistant".equals(role)) {
                    Object tc = m.get("tool_calls");
                    List<Object> tcl = Json.asList(tc);
                    if (tcl != null && !tcl.isEmpty()) {
                        // assistant 的 tool_calls → content blocks (tool_use)
                        List<Object> blocks = new ArrayList<>();
                        String content = Json.str(m.get("content"));
                        if (content != null && !content.isEmpty()) {
                            blocks.add(Json.obj("type", "text", "text", content));
                        }
                        for (Object tco : tcl) {
                            Map<String, Object> t = Json.asMap(tco);
                            if (t == null) continue;
                            Map<String, Object> fn = Json.asMap(t.get("function"));
                            String argsStr = fn == null ? "{}" : Json.str(fn.get("arguments"));
                            Object args;
                            try { args = Json.parse(argsStr == null ? "{}" : argsStr); } catch (Exception e) { args = Json.obj(); }
                            blocks.add(Json.obj("type", "tool_use", "id", t.get("id"), "name", fn == null ? null : fn.get("name"), "input", args));
                        }
                        outMessages.add(Json.obj("role", "assistant", "content", blocks));
                        continue;
                    }
                }
                // 普通 user/assistant 消息：content 字符串或数组透传
                Map<String, Object> mm = new LinkedHashMap<>();
                mm.put("role", role);
                mm.put("content", m.get("content") == null ? "" : m.get("content"));
                outMessages.add(mm);
            }
        }
        if (outMessages.isEmpty()) outMessages.add(Json.obj("role", "user", "content", ""));
        out.put("messages", outMessages);

        // tools: OpenAI function 格式 → Anthropic input_schema
        List<Object> tools = Json.asList(req.get("tools"));
        if (tools != null && !tools.isEmpty()) {
            List<Object> outTools = new ArrayList<>();
            for (Object o : tools) {
                Map<String, Object> t = Json.asMap(o);
                Map<String, Object> fn = Json.asMap(t == null ? null : t.get("function"));
                if (fn == null) continue;
                Map<String, Object> at = Json.obj("name", fn.get("name"), "description",
                        fn.get("description") == null ? "" : fn.get("description"));
                if (fn.get("parameters") != null) at.put("input_schema", fn.get("parameters"));
                else at.put("input_schema", Json.obj("type", "object", "properties", Json.obj()));
                outTools.add(at);
            }
            out.put("tools", outTools);
            // tool_choice
            Object tc = req.get("tool_choice");
            if (tc instanceof String s) {
                if ("required".equals(s)) out.put("tool_choice", Json.obj("type", "any"));
                else if ("auto".equals(s)) out.put("tool_choice", Json.obj("type", "auto"));
                else if ("none".equals(s)) { /* 不传 tools 语义即 none */ }
            } else if (tc instanceof Map<?, ?> tcm) {
                Map<String, Object> fn = Json.asMap(((Map<String, Object>) tcm).get("function"));
                if (fn != null) out.put("tool_choice", Json.obj("type", "tool", "name", fn.get("name")));
            }
        }

        // temperature / top_p 透传
        if (req.get("temperature") != null) out.put("temperature", req.get("temperature"));
        if (req.get("top_p") != null) out.put("top_p", req.get("top_p"));
        if (req.get("stop") != null) {
            Object stop = req.get("stop");
            List<Object> stops = Json.asList(stop);
            if (stops == null && stop instanceof String s) stops = Json.arr(s);
            if (stops != null && !stops.isEmpty()) out.put("stop_sequences", stops);
        }
        // stream 由 forward 层决定，这里不设
        return out;
    }

    @Override
    public Map<String, String> upstreamHeaders() {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("Content-Type", "application/json");
        h.put("anthropic-version", "2023-06-01");
        return h;
    }

    @Override
    public Map<String, Object> transformResponse(Map<String, Object> body) {
        // Anthropic 非流式响应 → OpenAI 格式
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", "chatcmpl-" + body.getOrDefault("id", "gw"));
        out.put("object", "chat.completion");
        out.put("created", System.currentTimeMillis() / 1000);
        out.put("model", body.get("model"));

        List<Object> blocks = Json.asList(body.get("content"));
        StringBuilder text = new StringBuilder();
        List<Object> toolCalls = new ArrayList<>();
        int tcIndex = 0;
        if (blocks != null) {
            for (Object o : blocks) {
                Map<String, Object> b = Json.asMap(o);
                if (b == null) continue;
                String type = Json.str(b.get("type"));
                if ("text".equals(type)) text.append(b.get("text"));
                else if ("tool_use".equals(type)) {
                    Map<String, Object> fn = Json.obj("name", b.get("name"),
                            "arguments", Json.write(b.get("input") == null ? Json.obj() : b.get("input")));
                    toolCalls.add(Json.obj("id", b.get("id"), "type", "function", "index", tcIndex++, "function", fn));
                }
            }
        }
        Map<String, Object> msg = Json.obj("role", "assistant", "content", text.length() == 0 ? null : text.toString());
        if (!toolCalls.isEmpty()) msg.put("tool_calls", toolCalls);

        String stop = Json.str(body.get("stop_reason"));
        String finish = stop == null ? "stop" : switch (stop) {
            case "max_tokens" -> "length";
            case "tool_use" -> "tool_calls";
            case "stop_sequence" -> "stop";
            default -> "stop";
        };
        out.put("choices", Json.arr(Json.obj("index", 0, "message", msg, "finish_reason", finish)));

        Map<String, Object> usage = Json.asMap(body.get("usage"));
        Map<String, Object> outUsage = Json.obj();
        if (usage != null) {
            outUsage.put("prompt_tokens", usage.get("input_tokens"));
            outUsage.put("completion_tokens", usage.get("output_tokens"));
            outUsage.put("total_tokens", Json.longVal(usage.get("input_tokens")) == null ? null
                    : Json.longVal(usage.get("input_tokens")) + (Json.longVal(usage.get("output_tokens")) == null ? 0 : Json.longVal(usage.get("output_tokens"))));
        }
        out.put("usage", outUsage);
        return out;
    }

    // ================= 流式转换状态 =================
    private String model;
    private String respId;
    private final Map<String, Object> usageAcc = new LinkedHashMap<>();
    private boolean started = false;
    private final StringBuilder[] toolArgsBuf = new StringBuilder[32];
    private int currentToolIdx = -1;
    private String currentToolId;
    private String currentToolName;

    @Override
    public List<String> transformStreamEvent(String line) {
        // 上游行格式：event: xxx\ndata: {...}。forward 层按行喂给我们的是 "data:{...}" 或 "event:xxx"
        if (line == null || line.isBlank()) return List.of();
        String trimmed = line.strip();
        if (trimmed.startsWith("event:")) return List.of();  // 事件类型行，转换由 data 行驱动
        if (!trimmed.startsWith("data:")) return List.of();
        String payload = trimmed.substring(5).strip();
        if ("[DONE]".equals(payload)) return List.of();
        Map<String, Object> ev;
        try {
            ev = Json.parseObject(payload);
        } catch (Exception e) {
            return List.of();
        }
        String type = Json.str(ev.get("type"));
        if (type == null) return List.of();
        List<String> out = new ArrayList<>();
        long now = System.currentTimeMillis() / 1000;
        String oid = respId == null ? "chatcmpl-anthropic" : "chatcmpl-" + respId;

        switch (type) {
            case "message_start" -> {
                Map<String, Object> msg = Json.asMap(ev.get("message"));
                if (msg != null) {
                    model = Json.str(msg.get("model"));
                    respId = Json.str(msg.get("id"));
                    Map<String, Object> u = Json.asMap(msg.get("usage"));
                    if (u != null) {
                        usageAcc.put("prompt_tokens", u.get("input_tokens"));
                    }
                }
                Map<String, Object> chunk = baseChunk(oid, model);
                chunk.put("choices", Json.arr(Json.obj("index", 0, "delta", Json.obj("role", "assistant"), "finish_reason", null)));
                out.add("data: " + Json.write(chunk));
                started = true;
            }
            case "content_block_start" -> {
                int idx = Json.intVal(ev.get("index")) == null ? 0 : Json.intVal(ev.get("index"));
                Map<String, Object> block = Json.asMap(ev.get("content_block"));
                if (block == null) break;
                if ("tool_use".equals(Json.str(block.get("type")))) {
                    currentToolIdx = idx;
                    currentToolId = Json.str(block.get("id"));
                    currentToolName = Json.str(block.get("name"));
                    if (idx < toolArgsBuf.length) toolArgsBuf[idx] = new StringBuilder();
                    // tool_call 起始 chunk
                    Map<String, Object> chunk = baseChunk(oid, model);
                    Map<String, Object> fn = Json.obj("name", currentToolName, "arguments", "");
                    Map<String, Object> tc = Json.obj("index", 0, "id", currentToolId, "type", "function", "function", fn);
                    // OpenAI 规范：index 是 tool_calls 数组内下标
                    chunk.put("choices", Json.arr(Json.obj("index", 0,
                            "delta", Json.obj("tool_calls", Json.arr(tc)), "finish_reason", null)));
                    out.add("data: " + Json.write(chunk));
                }
                // text block 起始无需发 chunk（delta 开始才发）
            }
            case "content_block_delta" -> {
                Map<String, Object> delta = Json.asMap(ev.get("delta"));
                if (delta == null) break;
                String dtype = Json.str(delta.get("type"));
                int idx = Json.intVal(ev.get("index")) == null ? 0 : Json.intVal(ev.get("index"));
                if ("text_delta".equals(dtype)) {
                    Map<String, Object> chunk = baseChunk(oid, model);
                    chunk.put("choices", Json.arr(Json.obj("index", 0,
                            "delta", Json.obj("content", delta.get("text")), "finish_reason", null)));
                    out.add("data: " + Json.write(chunk));
                } else if ("input_json_delta".equals(dtype) && idx == currentToolIdx) {
                    String partial = Json.str(delta.get("partial_json"));
                    if (partial != null && currentToolIdx < toolArgsBuf.length && toolArgsBuf[currentToolIdx] != null) {
                        toolArgsBuf[currentToolIdx].append(partial);
                        Map<String, Object> chunk = baseChunk(oid, model);
                        Map<String, Object> fn = Json.obj("arguments", partial);
                        Map<String, Object> tc = Json.obj("index", 0, "function", fn);
                        chunk.put("choices", Json.arr(Json.obj("index", 0,
                                "delta", Json.obj("tool_calls", Json.arr(tc)), "finish_reason", null)));
                        out.add("data: " + Json.write(chunk));
                    }
                }
            }
            case "content_block_stop" -> {
                // 工具块结束：无需额外 chunk（参数已流式传完）
            }
            case "message_delta" -> {
                Map<String, Object> delta = Json.asMap(ev.get("delta"));
                Map<String, Object> u = Json.asMap(ev.get("usage"));
                if (u != null && u.get("output_tokens") != null) usageAcc.put("completion_tokens", u.get("output_tokens"));
                String stopReason = delta == null ? null : Json.str(delta.get("stop_reason"));
                String finish = stopReason == null ? "stop" : switch (stopReason) {
                    case "max_tokens" -> "length";
                    case "tool_use" -> "tool_calls";
                    default -> "stop";
                };
                Map<String, Object> chunk = baseChunk(oid, model);
                chunk.put("choices", Json.arr(Json.obj("index", 0, "delta", Json.obj(), "finish_reason", finish)));
                if (!usageAcc.isEmpty()) chunk.put("usage", usageAcc);
                out.add("data: " + Json.write(chunk));
            }
            case "message_stop" -> out.add("data: [DONE]");
            case "ping", "error" -> { }
            default -> { }
        }
        return out;
    }

    private Map<String, Object> baseChunk(String id, String model) {
        Map<String, Object> chunk = new LinkedHashMap<>();
        chunk.put("id", id);
        chunk.put("object", "chat.completion.chunk");
        chunk.put("created", System.currentTimeMillis() / 1000);
        chunk.put("model", model == null ? "anthropic" : model);
        return chunk;
    }

    @Override
    public Map<String, Object> extractUsage(Object lastEventData) {
        return usageAcc.isEmpty() ? null : usageAcc;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> aggregateStream(List<String> lines) {
        // 复用流式转换再聚合：把 Anthropic 事件流先转 OpenAI 事件流，再走 OpenAI 聚合
        OpenAiAdaptor openAi = new OpenAiAdaptor();
        List<String> openAiLines = new ArrayList<>();
        for (String l : lines) openAiLines.addAll(transformStreamEvent(l));
        openAiLines.addAll(streamFinish());
        return openAi.aggregateStream(openAiLines);
    }

    @Override
    public List<String> streamFinish() {
        return List.of("data: [DONE]");
    }

    @Override
    public boolean isRealFirstToken(String upstreamLine) {
        // Anthropic 上游：只有 content_block_delta(text_delta/thinking_delta/input_json_delta) 或带
        // stop_reason 的 message_delta 才是"真内容"。thinking_delta = 思考模型的真实输出活动，
        // 与 OpenAI 侧 reasoning_content 同口径算首字（当前 8 家上游均为 openai 协议，此路备用）。
        // message_start / content_block_start / ping 只是握手与元信息，
        // 不能当作首字——否则慢模型靠握手事件就能通过 TTFT 检查、霸占并发位无法 failover。
        if (upstreamLine == null || upstreamLine.isBlank()) return false;
        String trimmed = upstreamLine.strip();
        if (!trimmed.startsWith("data:")) return false;
        String payload = trimmed.substring(5).strip();
        if ("[DONE]".equals(payload)) return false;
        try {
            Map<String, Object> ev = Json.parseObject(payload);
            String type = Json.str(ev.get("type"));
            if (type == null) return false;
            if ("content_block_delta".equals(type)) {
                Map<String, Object> delta = Json.asMap(ev.get("delta"));
                String dtype = delta == null ? null : Json.str(delta.get("type"));
                return "text_delta".equals(dtype) || "thinking_delta".equals(dtype)
                        || "input_json_delta".equals(dtype);
            }
            if ("message_delta".equals(type)) {
                Map<String, Object> delta = Json.asMap(ev.get("delta"));
                return delta != null && delta.get("stop_reason") != null;
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }
}
