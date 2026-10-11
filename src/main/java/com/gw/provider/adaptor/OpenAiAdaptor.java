package com.gw.provider.adaptor;

import java.util.*;

/**
 * OpenAI 兼容适配器：透传为主，仅做模型名替换与 usage 接住。
 */
public class OpenAiAdaptor implements ProtocolAdaptor {

    @Override
    public String protocol() { return "openai"; }

    @Override
    public Map<String, Object> transformRequest(Map<String, Object> req, String realModel) {
        // 透传 + 模型名替换（不做白名单过滤，保持 max_tokens/stream_options 等参数语义——设计文档铁律）
        Map<String, Object> out = new LinkedHashMap<>(req);
        out.put("model", realModel);
        return out;
    }

    @Override
    public Map<String, String> upstreamHeaders() {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("Content-Type", "application/json");
        return h;
    }

    @Override
    public Map<String, Object> transformResponse(Map<String, Object> body) {
        return body; // 原生即 OpenAI 格式
    }

    @Override
    public List<String> transformStreamEvent(String line) {
        // 原样透传（含 data: [DONE]）
        return line == null || line.isEmpty() ? List.of() : List.of(line);
    }

    @Override
    public boolean isRealFirstToken(String upstreamLine) {
        // OpenAI 上游：只有 delta 含非空 content 或 tool_calls 才算真首字。
        // 空 delta / role-only / keep-alive 注释行都只是"连接活着"，不代表已开始产出。
        return ProtocolAdaptor.openAiDataHasContent(upstreamLine);
    }

    @Override
    public Map<String, Object> extractUsage(Object lastEventData) {
        // OpenAI: 流式最后一个 chunk 带 usage（stream_options.include_usage），非流式 body 也有
        if (lastEventData instanceof Map<?, ?> m) {
            Object usage = m.get("usage");
            if (usage instanceof Map<?, ?> u) return castMap(u);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> m) {
        return (Map<String, Object>) m;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> aggregateStream(List<String> lines) {
        // 收集所有 delta content/tool_calls，聚合成完整响应
        StringBuilder content = new StringBuilder();
        String model = null, finishReason = null, id = null;
        List<Object> toolCalls = new ArrayList<>();
        Map<String, Object> usage = null;
        List<Map<String, Object>> mergedTools = new ArrayList<>();
        for (String raw : lines) {
            if (raw == null || !raw.startsWith("data:")) continue;
            String payload = raw.substring(5).strip();
            if (payload.equals("[DONE]")) continue;
            try {
                Map<String, Object> chunk = Json.parseObject(payload);
                if (model == null) model = Json.str(chunk.get("model"));
                if (id == null) id = Json.str(chunk.get("id"));
                if (chunk.get("usage") instanceof Map<?, ?> u) usage = castMap(u);
                Object choices = chunk.get("choices");
                List<Object> cl = Json.asList(choices);
                if (cl != null && !cl.isEmpty()) {
                    Map<String, Object> choice = Json.asMap(cl.get(0));
                    if (choice == null) continue;
                    if (choice.get("finish_reason") != null) finishReason = Json.str(choice.get("finish_reason"));
                    Object delta = choice.get("delta");
                    Map<String, Object> d = Json.asMap(delta);
                    if (d == null) continue;
                    if (d.get("content") instanceof String s) content.append(s);
                    Object tc = d.get("tool_calls");
                    List<Object> tcl = Json.asList(tc);
                    if (tcl != null) {
                        for (Object o : tcl) {
                            Map<String, Object> t = Json.asMap(o);
                            if (t == null) continue;
                            int idx = t.get("index") == null ? 0 : Json.intVal(t.get("index"));
                            while (mergedTools.size() <= idx) mergedTools.add(new LinkedHashMap<>());
                            Map<String, Object> mt = mergedTools.get(idx);
                            if (t.get("id") != null) mt.put("id", t.get("id"));
                            if (t.get("type") != null) mt.put("type", t.get("type"));
                            Map<String, Object> fn = Json.asMap(t.get("function"));
                            if (fn != null) {
                                Map<String, Object> mfn = Json.asMap(mt.get("function"));
                                if (mfn == null) { mfn = new LinkedHashMap<>(); mt.put("function", mfn); }
                                if (fn.get("name") != null) mfn.put("name", fn.get("name"));
                                if (fn.get("arguments") instanceof String a) {
                                    mfn.merge("arguments", a, (a1, a2) -> String.valueOf(a1) + a2);
                                }
                            }
                        }
                    }
                }
            } catch (Exception ignore) {
            }
        }
        if (!mergedTools.isEmpty()) toolCalls.addAll(mergedTools);
        Map<String, Object> msg = Json.obj("role", "assistant", "content", content.length() == 0 ? null : content.toString());
        if (!toolCalls.isEmpty()) msg.put("tool_calls", toolCalls);
        Map<String, Object> out = Json.obj(
                "id", id == null ? "chatcmpl-gw" : id,
                "object", "chat.completion",
                "created", System.currentTimeMillis() / 1000,
                "model", model == null ? "unknown" : model,
                "choices", Json.arr(Json.obj("index", 0, "message", msg, "finish_reason", finishReason == null ? "stop" : finishReason)));
        if (usage != null) out.put("usage", usage);
        return out;
    }
}
