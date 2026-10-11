package com.gw;

import com.gw.forward.ForwardEngine;
import com.gw.provider.adaptor.AnthropicConverter;
import com.gw.provider.adaptor.Json;

import java.util.List;
import java.util.Map;

/**
 * 快速单测（main 运行）：验证 v7.2 三个转换修复。
 * 1. Anthropic tool_result 多轮 → OpenAI tool 消息紧跟 assistant(tool_calls)；
 * 2. 空 assistant 轮（纯 tool_use 无文本）不丢消息；
 * 3. normalizeMessages 孤儿 tool → user 兜底 + 缺的 tool_call_id 补空。
 */
public class ConverterToolFlowTest {
    public static void main(String[] args) throws Exception {
        // ---- 1. Anthropic 请求：assistant 纯 tool_use 轮 + user tool_result 轮 ----
        Map<String, Object> req = Json.parseObject("""
            {
              "model": "m", "max_tokens": 100,
              "messages": [
                {"role":"user","content":"read the file"},
                {"role":"assistant","content":[{"type":"tool_use","id":"tu1","name":"read","input":{"path":"a.txt"}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"tu1","content":"file body here"},{"type":"text","text":"now summarize"}]},
                {"role":"assistant","content":[{"type":"text","text":"summary done"}]}
              ]
            }""");
        Map<String, Object> openai = AnthropicConverter.anthropicRequestToOpenai(req, "m");
        List<Object> msgs = Json.asList(openai.get("messages"));
        StringBuilder seq = new StringBuilder();
        for (Object o : msgs) {
            Map<String, Object> m = Json.asMap(o);
            seq.append(Json.str(m.get("role")));
            if (m.get("tool_calls") != null) seq.append("+tc");
            if (m.get("tool_call_id") != null) seq.append("(").append(Json.str(m.get("tool_call_id"))).append(")");
            seq.append(" ");
        }
        System.out.println("seq1: " + seq);
        // 期望：user assistant+tc tool(tu1) user assistant
        check("1 tool 消息紧跟 assistant(tool_calls)",
                seq.toString().equals("user assistant+tc tool(tu1) user assistant "), seq.toString());

        // ---- 2. 空 assistant 轮保留 ----
        Map<String, Object> req2 = Json.parseObject("""
            { "model":"m","max_tokens":10,
              "messages":[
                {"role":"user","content":"hi"},
                {"role":"assistant","content":[]},
                {"role":"user","content":"again"}
              ]}""");
        Map<String, Object> o2 = AnthropicConverter.anthropicRequestToOpenai(req2, "m");
        List<Object> m2 = Json.asList(o2.get("messages"));
        System.out.println("seq2 roles: " + m2.stream().map(x -> Json.str(Json.asMap(x).get("role"))).toList());
        check("2 空 assistant 轮保留", m2.size() == 3 && "assistant".equals(Json.str(Json.asMap(m2.get(1)).get("role"))),
                String.valueOf(m2.size()));

        // ---- 3. normalizeMessages：孤儿 tool + 缺结果补空 ----
        Map<String, Object> native3 = Json.parseObject("""
            { "model":"m",
              "messages":[
                {"role":"system","content":"sys"},
                {"role":"user","content":"u1"},
                {"role":"tool","tool_call_id":"orphan1","content":"lost result"},
                {"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
                {"role":"user","content":"u2 mid"}
              ]}""");
        java.lang.reflect.Method nm = ForwardEngine.class.getDeclaredMethod("normalizeMessages", Map.class);
        nm.setAccessible(true);
        nm.invoke(null, native3);
        List<Object> m3 = Json.asList(native3.get("messages"));
        StringBuilder seq3 = new StringBuilder();
        for (Object o : m3) {
            Map<String, Object> m = Json.asMap(o);
            seq.append(Json.str(m.get("role")));
            if (m.get("tool_call_id") != null) seq3.append(Json.str(m.get("role"))).append("(")
                    .append(Json.str(m.get("tool_call_id"))).append(")");
            else if (m.get("tool_calls") != null) seq3.append("assistant+tc");
            else seq3.append(Json.str(m.get("role")));
            seq3.append(" ");
        }
        System.out.println("seq3: " + seq3);
        // 期望：system user user(孤儿转user) assistant+tc tool(c1 补空) user
        check("3 孤儿tool转user + 缺失id补空",
                seq3.toString().equals("system user user assistant+tc tool(c1) user "), seq3.toString());

        // ---- 4. thinking 块回填 reasoning_content（实测：lingxiongkeji 的 deepseek-v4-* 默认开思考，
        //      历史 assistant(tool_calls) 不带思考内容就 400 "must be passed back"）----
        Map<String, Object> req4 = Json.parseObject("""
            { "model":"m","max_tokens":100,
              "messages":[
                {"role":"user","content":"add 2 and 3 with the tool"},
                {"role":"assistant","content":[
                  {"type":"thinking","thinking":"the user wants a sum","signature":"sig"},
                  {"type":"tool_use","id":"tu9","name":"calc","input":{"a":2,"b":3}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"tu9","content":"5"}]}
              ]}""");
        Map<String, Object> o4 = AnthropicConverter.anthropicRequestToOpenai(req4, "m");
        List<Object> m4 = Json.asList(o4.get("messages"));
        Map<String, Object> asst4 = null;
        for (Object o : m4) {
            Map<String, Object> m = Json.asMap(o);
            if ("assistant".equals(Json.str(m.get("role"))) && m.get("tool_calls") != null) asst4 = m;
        }
        check("4 assistant(tool_calls) 带 reasoning_content",
                asst4 != null && "the user wants a sum".equals(Json.str(asst4.get("reasoning_content"))),
                String.valueOf(asst4));
        // 客户端没回传思考时不应凭空造字段（避免给不收该字段的上游添新形状）
        Map<String, Object> req4b = Json.parseObject("""
            { "model":"m","max_tokens":100,
              "messages":[
                {"role":"user","content":"add"},
                {"role":"assistant","content":[{"type":"tool_use","id":"t","name":"calc","input":{}}]}
              ]}""");
        List<Object> m4b = Json.asList(AnthropicConverter.anthropicRequestToOpenai(req4b, "m").get("messages"));
        Map<String, Object> asst4b = Json.asMap(m4b.get(1));
        check("4b 无思考块时不造 reasoning_content 字段", !asst4b.containsKey("reasoning_content"),
                String.valueOf(asst4b));

        // ---- 5. normalizeMessages 不吞 reasoning_content（OpenAI 入口客户端回传的要原样出到上游）----
        Map<String, Object> native5 = Json.parseObject("""
            { "model":"m","messages":[
                {"role":"user","content":"u"},
                {"role":"assistant","content":"","reasoning_content":"rc keep me",
                 "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
                {"role":"tool","tool_call_id":"c1","content":"r"}]}""");
        nm.invoke(null, native5);
        Map<String, Object> a5 = Json.asMap(Json.asList(native5.get("messages")).get(1));
        check("5 规范化后 reasoning_content 仍在",
                "rc keep me".equals(Json.str(a5.get("reasoning_content"))) && a5.get("tool_calls") != null,
                String.valueOf(a5));

        // ---- 6. injectThinkingDisable：名单/配置/自愈重试 三档 + 返回"是否已关思考" ----
        java.lang.reflect.Method itd = ForwardEngine.class.getDeclaredMethod(
                "injectThinkingDisable", com.gw.config.GatewayConfig.Provider.class, Map.class, boolean.class);
        itd.setAccessible(true);
        check("6 名单内 sensenova 关思考", Boolean.TRUE.equals(itd.invoke(null, pv("sensenova", "openai", "auto"),
                Json.parseObject("{\"model\":\"m\"}"), false)), "应返回 true");
        Map<String, Object> amdReq = Json.parseObject("{\"model\":\"m\"}");
        check("6 名单外(amd) auto 不注入", Boolean.FALSE.equals(itd.invoke(null, pv("amd", "openai", "auto"), amdReq, false))
                && !amdReq.containsKey("thinking"), String.valueOf(amdReq));
        Map<String, Object> cfgReq = Json.parseObject("{\"model\":\"m\"}");
        itd.invoke(null, pv("lingxiongkeji", "openai", "disable"), cfgReq, false);
        check("6 名单外但配了 disable → thinking disabled",
                "disabled".equals(Json.str(Json.asMap(cfgReq.get("thinking")).get("type"))), String.valueOf(cfgReq));
        Map<String, Object> healReq = Json.parseObject("{\"model\":\"m\"}");
        itd.invoke(null, pv("lingxiongkeji", "openai", "auto"), healReq, true);
        check("6 撞墙自愈(forceDisable) → thinking disabled",
                "disabled".equals(Json.str(Json.asMap(healReq.get("thinking")).get("type"))), String.valueOf(healReq));
        Map<String, Object> userReq = Json.parseObject("{\"model\":\"m\",\"reasoning_effort\":\"high\"}");
        check("6 客户端显式给思考开关则一律不覆盖",
                Boolean.FALSE.equals(itd.invoke(null, pv("sensenova", "openai", "disable"), userReq, true))
                        && "high".equals(Json.str(userReq.get("reasoning_effort"))), String.valueOf(userReq));
        Map<String, Object> anthReq = Json.parseObject("{\"model\":\"m\"}");
        check("6 anthropic 协议出站不注 OpenAI 形状开关",
                Boolean.FALSE.equals(itd.invoke(null, pv("newreseller", "anthropic", "disable"), anthReq, true))
                        && !anthReq.containsKey("thinking"), String.valueOf(anthReq));

        System.out.println("\n全部通过");
    }

    static com.gw.config.GatewayConfig.Provider pv(String id, String protocol, String thinkingMode) {
        com.gw.config.GatewayConfig.Provider p = new com.gw.config.GatewayConfig.Provider();
        p.setId(id);
        p.setProtocol(protocol);
        p.setThinkingMode(thinkingMode);
        return p;
    }

    static void check(String name, boolean ok, String detail) {
        System.out.println((ok ? "PASS " : "FAIL ") + name + (ok ? "" : "  <-- " + detail));
        if (!ok) System.exit(1);
    }
}
