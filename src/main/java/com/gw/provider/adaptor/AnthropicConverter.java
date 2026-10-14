package com.gw.provider.adaptor;

import java.util.*;

/**
 * Anthropic ⇄ OpenAI 双向转换器（接入侧）。
 *
 * 用途：Claude Code 发 Anthropic 原生协议 → 网关内部统一 OpenAI 格式 → 响应转回 Anthropic 格式。
 * 与 AnthropicAdaptor（出站方向，OpenAI→Anthropic）互为镜像。
 *
 * - anthropicRequestToOpenai: Anthropic 请求体 → OpenAI 请求体
 * - openaiResponseToAnthropic: OpenAI 非流式响应 → Anthropic 响应
 * - openaiStreamToAnthropic: OpenAI SSE chunk → Anthropic SSE 事件（有状态）
 */
public class AnthropicConverter {

    private AnthropicConverter() {}

    // ================= 请求：Anthropic → OpenAI =================

    @SuppressWarnings("unchecked")
    public static Map<String, Object> anthropicRequestToOpenai(Map<String, Object> in, String model) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("model", model);
        out.put("max_tokens", in.get("max_tokens") == null ? 4096 : in.get("max_tokens"));

        List<Object> outMessages = new ArrayList<>();
        // system（字符串或 content blocks）
        Object system = in.get("system");
        String systemText = flattenContent(system);
        if (systemText != null && !systemText.isEmpty()) {
            outMessages.add(Json.obj("role", "system", "content", systemText));
        }

        List<Object> messages = Json.asList(in.get("messages"));
        if (messages != null) {
            for (Object o : messages) {
                Map<String, Object> m = Json.asMap(o);
                if (m == null) continue;
                String role = Json.str(m.get("role"));
                Object content = m.get("content");
                List<Object> blocks = Json.asList(content);
                if (blocks == null) {
                    // 纯字符串消息
                    outMessages.add(Json.obj("role", role, "content", content == null ? "" : content));
                    continue;
                }
                // content blocks：text / thinking / tool_use / tool_result / image
                List<Object> toolResults = new ArrayList<>();
                StringBuilder text = new StringBuilder();
                StringBuilder reasoning = new StringBuilder();
                List<Object> assistantToolCalls = new ArrayList<>();
                for (Object bo : blocks) {
                    Map<String, Object> b = Json.asMap(bo);
                    if (b == null) continue;
                    String type = Json.str(b.get("type"));
                    if (type == null) continue;
                    switch (type) {
                        case "text" -> text.append(b.get("text") == null ? "" : b.get("text"));
                        case "thinking" -> {
                            // 思考块回填为 assistant.reasoning_content（此前直接丢弃）。默认开思考的上游
                            // （实测 lingxiongkeji 的 deepseek-v4-flash/pro）要求历史 tool_calls 轮把思考内容
                            // 带回来，丢了就 400 "reasoning_content in the thinking mode must be passed back"。
                            Object th = b.get("thinking");
                            if (th != null) reasoning.append(th);
                        }
                        case "tool_use" -> {
                            Map<String, Object> fn = Json.obj(
                                    "name", b.get("name"),
                                    "arguments", Json.write(b.get("input") == null ? Json.obj() : b.get("input")));
                            assistantToolCalls.add(Json.obj("id", b.get("id"), "type", "function", "function", fn));
                        }
                        case "tool_result" -> {
                            Map<String, Object> tr = Json.obj("role", "tool",
                                    "tool_call_id", b.get("tool_use_id"),
                                    "content", flattenToolResultContent(b.get("content")));
                            toolResults.add(tr);
                        }
                        case "image" -> {
                            Map<String, Object> source = Json.asMap(b.get("source"));
                            if (source != null) {
                                String dataUrl = "data:" + Json.str(source.get("media_type"))
                                        + ";base64," + Json.str(source.get("data"));
                                outMessages.add(Json.obj("role", "user", "content", Json.arr(
                                        Json.obj("type", "image_url", "image_url", Json.obj("url", dataUrl)))));
                            }
                        }
                        default -> { }
                    }
                }
                if ("assistant".equals(role)) {
                    // 工具调用轮：即使无文本也必须保留消息（tool_result 要跟在它后面），
                    // 否则后续 tool 消息找不到对应 tool_call_id → 上游 400
                    if (text.length() > 0 || !assistantToolCalls.isEmpty()) {
                        Map<String, Object> am = Json.obj("role", "assistant",
                                "content", text.length() == 0 ? "" : text.toString());
                        if (!assistantToolCalls.isEmpty()) am.put("tool_calls", assistantToolCalls);
                        if (reasoning.length() > 0) am.put("reasoning_content", reasoning.toString());
                        outMessages.add(am);
                    } else {
                        outMessages.add(Json.obj("role", "assistant", "content", ""));
                    }
                } else {
                    // OpenAI 严格要求 tool 消息紧跟 assistant(tool_calls)：
                    // 先出 tool 结果（把 tool_result 转成的消息），文本部分放其后，
                    // 否则 text 会插在 assistant(tool_calls) 与 tool 之间 → 上游 400
                    outMessages.addAll(toolResults);
                    toolResults.clear();
                    if (text.length() > 0) outMessages.add(Json.obj("role", role, "content", text.toString()));
                    continue;
                }
                outMessages.addAll(toolResults);
            }
        }
        if (outMessages.isEmpty()) outMessages.add(Json.obj("role", "user", "content", ""));
        out.put("messages", outMessages);

        // tools
        List<Object> tools = Json.asList(in.get("tools"));
        if (tools != null && !tools.isEmpty()) {
            List<Object> outTools = new ArrayList<>();
            for (Object o : tools) {
                Map<String, Object> t = Json.asMap(o);
                if (t == null) continue;
                Map<String, Object> fn = Json.obj("name", t.get("name"),
                        "description", t.get("description") == null ? "" : t.get("description"));
                if (t.get("input_schema") != null) fn.put("parameters", t.get("input_schema"));
                outTools.add(Json.obj("type", "function", "function", fn));
            }
            out.put("tools", outTools);
            Object tc = in.get("tool_choice");
            if (tc instanceof Map<?, ?> tcm) {
                Map<String, Object> t = (Map<String, Object>) tcm;
                String tct = Json.str(t.get("type"));
                if ("any".equals(tct)) out.put("tool_choice", "required");
                else if ("auto".equals(tct)) out.put("tool_choice", "auto");
                else if ("tool".equals(tct)) out.put("tool_choice",
                        Json.obj("type", "function", "function", Json.obj("name", t.get("name"))));
            }
        }

        if (in.get("temperature") != null) out.put("temperature", in.get("temperature"));
        if (in.get("top_p") != null) out.put("top_p", in.get("top_p"));
        if (in.get("stream") != null) out.put("stream", in.get("stream"));
        Object stop = in.get("stop_sequences");
        if (stop != null) out.put("stop", stop);
        return out;
    }

    /** Anthropic content（string 或 blocks）→ 纯文本 */
    @SuppressWarnings("unchecked")
    private static String flattenContent(Object content) {
        if (content == null) return null;
        if (content instanceof String s) return s;
        List<Object> blocks = (List<Object>) content;
        StringBuilder sb = new StringBuilder();
        for (Object o : blocks) {
            Map<String, Object> b = (Map<String, Object>) o;
            if ("text".equals(b.get("type"))) sb.append(b.get("text"));
        }
        return sb.toString();
    }

    private static Object flattenToolResultContent(Object content) {
        // tool_result 的 content 可能是 string 或 blocks（含图片时保留为文本描述）
        String flat = flattenContent(content);
        if (flat != null) return flat;
        if (content instanceof List) return Json.write(content);
        return content == null ? "" : String.valueOf(content);
    }

    // ================= 非流式响应：OpenAI → Anthropic =================

    @SuppressWarnings("unchecked")
    public static Map<String, Object> openaiResponseToAnthropic(Map<String, Object> r, String requestModel) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", "msg_" + (r.get("id") == null ? "gw" : r.get("id")));
        out.put("type", "message");
        out.put("role", "assistant");
        out.put("model", requestModel);

        List<Object> choices = (List<Object>) r.get("choices");
        List<Object> content = new ArrayList<>();
        String finishReason = "stop";
        if (choices != null && !choices.isEmpty()) {
            Map<String, Object> choice = (Map<String, Object>) choices.get(0);
            Map<String, Object> msg = (Map<String, Object>) choice.get("message");
            if (msg != null) {
                // 思考过程 → thinking 块（Claude Code 显示用）
                Object reasoning = msg.get("reasoning_content");
                if (reasoning instanceof String rs && !rs.isEmpty()) {
                    content.add(Json.obj("type", "thinking", "thinking", rs));
                }
                Object text = msg.get("content");
                if (text instanceof String s && !s.isEmpty()) {
                    content.add(Json.obj("type", "text", "text", s));
                }
                List<Object> toolCalls = (List<Object>) msg.get("tool_calls");
                if (toolCalls != null) {
                    for (Object o : toolCalls) {
                        Map<String, Object> tc = (Map<String, Object>) o;
                        Map<String, Object> fn = (Map<String, Object>) tc.get("function");
                        // arguments 兼容两种上游形态：String 解析；已是对象直接用
                        //（旧强转 (String) 遇对象形抛 ClassCastException 被吞成空 input → 工具参数全丢）
                        Object rawArgs = fn == null ? null : fn.get("arguments");
                        Object input;
                        if (rawArgs instanceof String s) {
                            try { input = Json.parse(s); } catch (Exception e) { input = Json.obj(); }
                        } else if (rawArgs != null) {
                            input = rawArgs;
                        } else {
                            input = Json.obj();
                        }
                        content.add(Json.obj("type", "tool_use", "id", tc.get("id"),
                                "name", fn.get("name"), "input", input));
                    }
                }
            }
            Object fr = choice.get("finish_reason");
            if (fr != null) {
                finishReason = switch (String.valueOf(fr)) {
                    case "length" -> "max_tokens";
                    case "tool_calls" -> "tool_use";
                    default -> "end_turn";
                };
            }
        }
        out.put("content", content);
        out.put("stop_reason", finishReason);
        out.put("stop_sequence", null);

        Map<String, Object> usage = (Map<String, Object>) r.get("usage");
        Map<String, Object> outUsage = new LinkedHashMap<>();
        outUsage.put("input_tokens", usage == null || usage.get("prompt_tokens") == null ? 0 : usage.get("prompt_tokens"));
        outUsage.put("output_tokens", usage == null || usage.get("completion_tokens") == null ? 0 : usage.get("completion_tokens"));
        out.put("usage", outUsage);
        return out;
    }

    // ================= 流式：OpenAI chunk → Anthropic 事件（有状态） =================

    /** 每个流式请求一个实例 */
    public static class StreamState {
        public boolean messageStartSent = false;
        public boolean blockStarted = false;
        public boolean finished = false;               // 已发过 message_stop（防重复收尾）
        public int blockIdx = 0;
        public String blockType = null;          // "线性"当前块：text / thinking（并行工具块不占此槽）
        /** 并行多工具各占独立槽：key 上游带 index 用 "n"+index，否则 "#"+id；
         *  无 id/index 的纯续传增量并入 lastToolKey。单槽缓冲会把第二个工具的参数拼进第一个工具。 */
        public final LinkedHashMap<String, ToolSlot> toolSlots = new LinkedHashMap<>();
        public String lastToolKey = null;
        public long outputTokens = 0;
        public final Map<String, Object> usageAcc = new LinkedHashMap<>();

        public static class ToolSlot {
            public int anthrIdx;                 // Anthropic content block 下标
            public String id = "";
            public final StringBuilder args = new StringBuilder();
        }
    }

    /** 关闭当前"线性"块 + 所有仍未关闭的工具块（工具块 start 即发、收尾统一 stop），返回事件行 */
    public static List<String> closeOpenBlocks(StreamState st) {
        List<String> out = new ArrayList<>();
        if (st.blockStarted) {
            out.add("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":" + st.blockIdx + "}\n");
            st.blockStarted = false;
        }
        for (StreamState.ToolSlot s : st.toolSlots.values()) {
            out.add("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":" + s.anthrIdx + "}\n");
        }
        return out;
    }

    /** 网关主动掐断一条已提交(200)的流时的收尾帧：先关掉所有未闭合块，再发 error 事件。
     *  刻意**不**发 message_delta(stop_reason=end_turn)/message_stop：那等于把"中断"谎报成
     *  "这轮答完了"，客户端就静默早停（2026-10-14 实测 3 发 amd/Qwen3.8-27B 被心跳掐断后
     *  一个字节都没写，Claude Code 收到没有 message_stop 的半截流）。发 error 才能让客户端
     *  明确报错/重试。置 finished 防重复收尾。 */
    public static List<String> abortFrames(StreamState st, String message) {
        List<String> out = new ArrayList<>(closeOpenBlocks(st));
        st.finished = true;
        out.add("event: error\ndata: " + Json.write(Json.obj("type", "error",
                "error", Json.obj("type", "api_error", "message", message))) + "\n");
        return out;
    }

    /**
     * OpenAI SSE 行（data: {...} 或 data: [DONE]）→ Anthropic SSE 事件行列表。
     * 每条只含 "event: xxx\ndata: {...}\n"；SSE 事件分隔空行由写出方（AnthropicController）补齐
     */
    @SuppressWarnings("unchecked")
    public static List<String> openaiChunkToAnthropicEvents(String openAiLine, StreamState st, String model) {
        if (openAiLine == null || openAiLine.isBlank()) return List.of();
        String t = openAiLine.strip();
        if (!t.startsWith("data:")) return List.of();
        String payload = t.substring(5).strip();
        if ("[DONE]".equals(payload)) {
            // 收尾：关当前块 + message_delta + message_stop（finished 防重复）
            List<String> out = new ArrayList<>();
            if (st.finished) return List.of();
            st.finished = true;
            out.addAll(closeOpenBlocks(st));
            Map<String, Object> u = Json.obj("output_tokens", Math.max(1, st.outputTokens));
            // 无 finish_reason 直接 [DONE]：流内出现过工具块必须报 tool_use（同 EOF 补收尾的理由）
            out.add("event: message_delta\ndata: " + Json.write(Json.obj("type", "message_delta",
                    "delta", Json.obj("stop_reason", st.toolSlots.isEmpty() ? "end_turn" : "tool_use",
                            "stop_sequence", null), "usage", u)) + "\n");
            out.add("event: message_stop\ndata: {\"type\":\"message_stop\"}\n");
            return out;
        }
        Map<String, Object> chunk;
        try {
            chunk = Json.parseObject(payload);
        } catch (Exception e) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        String mid = "msg_" + Json.str(chunk.getOrDefault("id", "gw"));

        // 任何 chunk 上的 usage 都接住（DeepSeek 等在末尾 usage chunk 给，choices 为空）
        Object uObj = chunk.get("usage");
        if (uObj instanceof Map<?, ?> um) {
            @SuppressWarnings("unchecked")
            Map<String, Object> umm = (Map<String, Object>) um;
            if (umm.get("prompt_tokens") != null) st.usageAcc.put("prompt_tokens", umm.get("prompt_tokens"));
            if (umm.get("completion_tokens") != null) st.outputTokens = Json.longVal(umm.get("completion_tokens"));
        }

        if (!st.messageStartSent) {
            st.messageStartSent = true;
            Map<String, Object> usage0 = Json.asMap(chunk.get("usage"));
            Map<String, Object> startUsage = Json.obj("input_tokens",
                    usage0 != null && usage0.get("prompt_tokens") != null ? usage0.get("prompt_tokens") : 0,
                    "output_tokens", 0);
            Map<String, Object> msg = Json.obj("id", mid, "type", "message", "role", "assistant",
                    "model", model, "content", Json.arr(), "stop_reason", null, "stop_sequence", null,
                    "usage", startUsage);
            out.add("event: message_start\ndata: " + Json.write(Json.obj("type", "message_start", "message", msg)) + "\n");
        }

        List<Object> choices = Json.asList(chunk.get("choices"));
        if (choices == null || choices.isEmpty()) {
            return out;  // 末尾 usage chunk：已在上面积累，无需生成事件
        }
        Map<String, Object> choice = Json.asMap(choices.get(0));
        Map<String, Object> delta = Json.asMap(choice.get("delta"));

        if (delta != null) {
            // 思考增量 → thinking 块
            Object rc = delta.get("reasoning_content");
            if (rc instanceof String rs && !rs.isEmpty()) {
                if (!"thinking".equals(st.blockType)) {
                    if (st.blockStarted) {
                        out.add("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":" + st.blockIdx + "}\n");
                        st.blockIdx++;
                    }
                    st.blockType = "thinking";
                    st.blockStarted = true;
                    out.add("event: content_block_start\ndata: " + Json.write(Json.obj("type", "content_block_start",
                            "index", st.blockIdx, "content_block", Json.obj("type", "thinking", "thinking", ""))) + "\n");
                }
                out.add("event: content_block_delta\ndata: " + Json.write(Json.obj("type", "content_block_delta",
                        "index", st.blockIdx, "delta", Json.obj("type", "thinking_delta", "thinking", rs))) + "\n");
            }
            // 文本增量
            Object c = delta.get("content");
            if (c instanceof String s && !s.isEmpty()) {
                if (!"text".equals(st.blockType)) {
                    if (st.blockStarted) {
                        out.add("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":" + st.blockIdx + "}\n");
                        st.blockIdx++;
                    }
                    st.blockType = "text";
                    st.blockStarted = true;
                    out.add("event: content_block_start\ndata: " + Json.write(Json.obj("type", "content_block_start",
                            "index", st.blockIdx, "content_block", Json.obj("type", "text", "text", ""))) + "\n");
                }
                out.add("event: content_block_delta\ndata: " + Json.write(Json.obj("type", "content_block_delta",
                        "index", st.blockIdx, "delta", Json.obj("type", "text_delta", "text", s))) + "\n");
            }
            // 工具调用增量：按 index（次为 id）定位独立块槽——并行多工具、增量不带 id、
            // 交错重复 id 三种上游行为下各归各块；arguments 兼容 String/Map 两种形态
            List<Object> toolCalls = Json.asList(delta.get("tool_calls"));
            if (toolCalls != null && !toolCalls.isEmpty()) {
                for (Object o : toolCalls) {
                    Map<String, Object> tc = Json.asMap(o);
                    if (tc == null) continue;
                    Map<String, Object> fn = Json.asMap(tc.get("function"));
                    String id = Json.str(tc.get("id"));
                    if (id == null) id = "";
                    String key = null;
                    StreamState.ToolSlot slot = null;
                    if (tc.get("index") instanceof Number n) {
                        key = "n" + n.intValue();
                        StreamState.ToolSlot s = st.toolSlots.get(key);
                        // 同 index 换了 id（上游按工具重置 index）→ 视为新工具，另开块
                        if (s != null && (id.isEmpty() || id.equals(s.id))) slot = s;
                    }
                    if (slot == null && !id.isEmpty()) {
                        for (Map.Entry<String, StreamState.ToolSlot> e : st.toolSlots.entrySet()) {
                            if (id.equals(e.getValue().id)) { slot = e.getValue(); key = e.getKey(); break; }
                        }
                        // 无 index 且 id 从未见过 → 新工具另开块（index 键必须保留，否则后续纯 index 续传找不到槽）
                        if (slot == null && key == null) key = "#" + id;
                    } else if (slot == null && key == null && st.lastToolKey != null) {
                        // 无 id 无 index 的纯续传增量：并入最近打开的工具块
                        key = st.lastToolKey;
                        slot = st.toolSlots.get(key);
                    }
                    if (slot == null) {
                        if (key == null) key = "#noname" + st.toolSlots.size();
                        if (st.blockStarted) {   // 先关当前 text/thinking 块再开工具块
                            out.add("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":" + st.blockIdx + "}\n");
                            st.blockIdx++;
                            st.blockStarted = false;
                            st.blockType = null;
                        }
                        slot = new StreamState.ToolSlot();
                        slot.anthrIdx = st.blockIdx++;
                        slot.id = id.isEmpty() ? "toolu_gw_" + slot.anthrIdx : id;   // 有 index 无 id 的兜底
                        st.toolSlots.put(key, slot);
                        String name = fn == null ? "" : Json.str(fn.get("name"));
                        out.add("event: content_block_start\ndata: " + Json.write(Json.obj("type", "content_block_start",
                                "index", slot.anthrIdx,
                                "content_block", Json.obj("type", "tool_use", "id", slot.id,
                                        "name", name == null ? "" : name, "input", Json.obj()))) + "\n");
                    }
                    st.lastToolKey = key;
                    String argsDelta = null;
                    Object argObj = fn == null ? null : fn.get("arguments");
                    if (argObj instanceof String s && !s.isEmpty()) argsDelta = s;
                    else if (argObj instanceof Map || argObj instanceof List) argsDelta = Json.write(argObj);
                    if (argsDelta != null) {
                        slot.args.append(argsDelta);
                        out.add("event: content_block_delta\ndata: " + Json.write(Json.obj("type", "content_block_delta",
                                "index", slot.anthrIdx,
                                "delta", Json.obj("type", "input_json_delta", "partial_json", argsDelta))) + "\n");
                    }
                }
            }
        }

        // finish_reason → 关块 + message_delta（一次性；finished 后不再处理）
        // 关键：sensenova 等每条内容 chunk 都带 "finish_reason":""（空串占位，非 null）。
        // 若按 fr!=null 判定会在第一条内容就误收尾 → 客户端"半句就停"。空串不算终止信号。
        Object frObj = choice.get("finish_reason");
        String fr = frObj == null ? null : String.valueOf(frObj);
        if (fr != null && !fr.isBlank() && !st.finished) {
            st.finished = true;
            String stop = switch (fr) {
                case "length" -> "max_tokens";
                case "tool_calls" -> "tool_use";
                default -> "end_turn";
            };
            out.addAll(closeOpenBlocks(st));
            out.add("event: message_delta\ndata: " + Json.write(Json.obj("type", "message_delta",
                    "delta", Json.obj("stop_reason", stop, "stop_sequence", null),
                    "usage", Json.obj("output_tokens", Math.max(1, st.outputTokens)))) + "\n");
            out.add("event: message_stop\ndata: {\"type\":\"message_stop\"}\n");
        }
        return out;
    }
}
