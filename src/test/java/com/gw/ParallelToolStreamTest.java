package com.gw;

import com.gw.provider.adaptor.AnthropicConverter;
import com.gw.provider.adaptor.AnthropicConverter.StreamState;
import com.gw.provider.adaptor.Json;

import java.util.*;

/**
 * 快速单测（main 直跑）：#3 并行多工具流式转换多槽化后的三种真实上游场景回归。
 * 对照清单评审修正后的机制：主路径（每个工具首 chunk 带自己 id）本来就正常，
 * 真正会丢参数/串台的是——
 *   S1 续传 chunk 不带 id（vLLM 按 index 约定发增量）；
 *   S2 arguments 非 String（对象形）；
 *   S3 交错/重复 id 的上游。
 * 外加理想顺序流与 EOF/[DONE] 收尾断言（每个开过的块都必须有 content_block_stop）。
 */
public class ParallelToolStreamTest {

    static int failures = 0;

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    /** 喂一批 data: 行，返回全部产出事件 */
    static List<Map<String, Object>> feed(StreamState st, List<String> lines) {
        List<Map<String, Object>> evs = new ArrayList<>();
        for (String l : lines) {
            for (String e : AnthropicConverter.openaiChunkToAnthropicEvents("data: " + l, st, "m")) {
                String data = e.substring(e.indexOf("data:") + 5).strip();
                Map<String, Object> ev = Json.parseObject(data);
                String type = Json.str(ev.get("type"));
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("event", type);
                if (ev.get("index") != null) row.put("index", Json.longVal(ev.get("index")));
                if ("content_block_start".equals(type)) {
                    Map<String, Object> cb = Json.asMap(ev.get("content_block"));
                    row.put("cbType", Json.str(cb.get("type")));
                    row.put("id", Json.str(cb.get("id")));
                    row.put("name", Json.str(cb.get("name")));
                }
                if ("content_block_delta".equals(type)) {
                    Map<String, Object> d = Json.asMap(ev.get("delta"));
                    row.put("dType", Json.str(d.get("type")));
                    if ("input_json_delta".equals(Json.str(d.get("type")))) {
                        String pj = Json.str(d.get("partial_json"));
                        row.put("partial_json", pj);
                    }
                }
                if ("message_delta".equals(type)) {
                    Map<String, Object> d = Json.asMap(ev.get("delta"));
                    row.put("stop_reason", Json.str(d.get("stop_reason")));
                }
                evs.add(row);
            }
        }
        return evs;
    }

    static String tcChunk(String tcJson, String finish) {
        return "{\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{" + tcJson + "}"
                + (finish == null ? "" : ",\"finish_reason\":\"" + finish + "\"") + "}]}";
    }

    /** 按事件流重建每个工具块的参数串：blockIdx → args */
    static Map<Long, StringBuilder> argsByBlock(List<Map<String, Object>> evs) {
        Map<Long, String> idxToId = new LinkedHashMap<>();
        Map<Long, StringBuilder> acc = new LinkedHashMap<>();
        for (Map<String, Object> e : evs) {
            if ("content_block_start".equals(e.get("event"))) {
                long idx = (Long) e.get("index");
                idxToId.put(idx, (String) e.get("id"));
                acc.put(idx, new StringBuilder());
            } else if ("content_block_delta".equals(e.get("event")) && "input_json_delta".equals(e.get("dType"))) {
                long idx = (Long) e.get("index");
                acc.computeIfAbsent(idx, k -> new StringBuilder()).append(e.get("partial_json"));
            }
        }
        return acc;
    }

    static long count(List<Map<String, Object>> evs, String event, String field, Object val) {
        return evs.stream().filter(e -> event.equals(e.get("event"))
                && (field == null || Objects.equals(val, e.get(field)))).count();
    }

    public static void main(String[] args) {
        // ---- S1：vLLM 式并行双工具，增量 chunk 不带 id、只带 index ----
        {
            StreamState st = new StreamState();
            List<Map<String, Object>> evs = feed(st, List.of(
                    tcChunk("\"tool_calls\":[{\"index\":0,\"id\":\"call_A\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"a.txt\\\"}\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"index\":1,\"id\":\"call_B\",\"type\":\"function\",\"function\":{\"name\":\"write\",\"arguments\":\"\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"b.txt\\\"}\"}}]", null),
                    tcChunk("\"tool_calls\":[]", "tool_calls")));
            Map<Long, StringBuilder> byIdx = argsByBlock(evs);
            check("S1 开出 2 个 tool_use 块", count(evs, "content_block_start", "cbType", "tool_use") == 2, evs.toString());
            check("S1 A 块参数完整", "{\"path\":\"a.txt\"}".contentEquals(byIdx.getOrDefault(0L, new StringBuilder())), String.valueOf(byIdx.get(0L)));
            check("S1 B 块参数完整（旧实现会拼进 A 且 B 无块）", "{\"path\":\"b.txt\"}".contentEquals(byIdx.getOrDefault(1L, new StringBuilder())), String.valueOf(byIdx.get(1L)));
            check("S1 收尾 stop_reason=tool_use", evs.stream().anyMatch(e -> "message_delta".equals(e.get("event")) && "tool_use".equals(e.get("stop_reason"))), "");
            check("S1 每个块都有 content_block_stop（含 finish 关块）",
                    count(evs, "content_block_stop", null, null) == 2, evs.toString());
        }

        // ---- S1b：增量连 index 都没有、但带 id（id 匹配归块）----
        {
            StreamState st = new StreamState();
            List<Map<String, Object>> evs = feed(st, List.of(
                    tcChunk("\"tool_calls\":[{\"id\":\"call_A\",\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"p\\\":1}\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"function\":{\"arguments\":\"{\\\"q\\\":2}\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"id\":\"call_B\",\"function\":{\"name\":\"write\",\"arguments\":\"{}\"}}]", null)));
            Map<Long, StringBuilder> byIdx = argsByBlock(evs);
            check("S1b 2 个块", byIdx.size() == 2, evs.toString());
            check("S1b 无 id 续传并入 A 块", "{\"p\":1}{\"q\":2}".contentEquals(byIdx.getOrDefault(0L, new StringBuilder())), String.valueOf(byIdx.get(0L)));
            check("S1b 带新 id 必须另开块", "{}".contentEquals(byIdx.getOrDefault(1L, new StringBuilder())), String.valueOf(byIdx.get(1L)));
        }

        // ---- S2：arguments 为对象形（非 String），流式不再静默丢弃 ----
        {
            StreamState st = new StreamState();
            List<Map<String, Object>> evs = feed(st, List.of(
                    tcChunk("\"tool_calls\":[{\"index\":0,\"id\":\"call_C\",\"function\":{\"name\":\"f\",\"arguments\":{\"k\":\"v\"}}}]", null),
                    tcChunk("\"tool_calls\":[]", "tool_calls")));
            Map<Long, StringBuilder> byIdx = argsByBlock(evs);
            check("S2 对象形 arguments 序列化下发", "{\"k\":\"v\"}".contentEquals(byIdx.getOrDefault(0L, new StringBuilder())), evs.toString());
            // 非流式：Map 形 arguments 不再 ClassCastException 吞成 {}
            Map<String, Object> resp = Json.parseObject("{\"id\":\"r\",\"model\":\"m\",\"choices\":[{\"index\":0,\"finish_reason\":\"tool_calls\","
                    + "\"message\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"id\":\"t1\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":{\"a\":1}}}]}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}");
            Map<String, Object> anth = AnthropicConverter.openaiResponseToAnthropic(resp, "m");
            List<Object> content = Json.asList(anth.get("content"));
            Map<String, Object> tu = Json.asMap(content.get(content.size() - 1));
            Map<String, Object> input = Json.asMap(tu.get("input"));
            check("S2 非流式对象形参数保留", input != null && "1".equals(String.valueOf(input.get("a"))), String.valueOf(tu));
        }

        // ---- S3：交错重复 id（B 增量之间插入 id=A）→ 不提前关 B、不重复开 A ----
        {
            StreamState st = new StreamState();
            List<Map<String, Object>> evs = feed(st, List.of(
                    tcChunk("\"tool_calls\":[{\"index\":0,\"id\":\"call_A\",\"function\":{\"name\":\"f1\",\"arguments\":\"{\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"index\":1,\"id\":\"call_B\",\"function\":{\"name\":\"f2\",\"arguments\":\"[\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"index\":0,\"id\":\"call_A\",\"function\":{\"arguments\":\"\\\"x\\\":1}\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"index\":1,\"id\":\"call_B\",\"function\":{\"arguments\":\"2]\"}}]", null),
                    tcChunk("\"tool_calls\":[]", "tool_calls")));
            check("S3 只开 2 个块（无重复开块）", count(evs, "content_block_start", "cbType", "tool_use") == 2, evs.toString());
            check("S3 无提前关块（stop 只在收尾）", count(evs, "content_block_stop", null, null) == 2, evs.toString());
            Map<Long, StringBuilder> byIdx = argsByBlock(evs);
            check("S3 A 参数聚合正确", "{\"x\":1}".contentEquals(byIdx.getOrDefault(0L, new StringBuilder())), String.valueOf(byIdx.get(0L)));
            check("S3 B 参数聚合正确", "[2]".contentEquals(byIdx.getOrDefault(1L, new StringBuilder())), String.valueOf(byIdx.get(1L)));
        }

        // ---- S4：理想顺序流（主路径不回归）+ [DONE] 收尾 stop_reason ----
        {
            StreamState st = new StreamState();
            List<Map<String, Object>> evs = feed(st, List.of(
                    tcChunk("\"tool_calls\":[{\"index\":0,\"id\":\"call_A\",\"function\":{\"name\":\"f1\",\"arguments\":\"{}\"}}]", null),
                    tcChunk("\"tool_calls\":[{\"index\":1,\"id\":\"call_B\",\"function\":{\"name\":\"f2\",\"arguments\":\"{}\"}}]", null),
                    "[DONE]"));
            Map<Long, StringBuilder> byIdx = argsByBlock(evs);
            check("S4 理想流 2 块", byIdx.size() == 2, evs.toString());
            check("S4 [DONE] 无 finish 时见过工具必须报 tool_use",
                    evs.stream().anyMatch(e -> "message_delta".equals(e.get("event")) && "tool_use".equals(e.get("stop_reason"))), evs.toString());
            check("S4 [DONE] 前关掉全部工具块", count(evs, "content_block_stop", null, null) == 2, evs.toString());
        }

        // ---- S5：纯文本流不回归（stop_reason=end_turn、单块单 stop）----
        {
            StreamState st = new StreamState();
            List<Map<String, Object>> evs = feed(st, List.of(
                    "{\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"}}]}",
                    "{\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}",
                    "[DONE]"));
            check("S5 文本块 1 个", count(evs, "content_block_start", "cbType", "text") == 1, evs.toString());
            check("S5 stop=end_turn", evs.stream().anyMatch(e -> "message_delta".equals(e.get("event")) && "end_turn".equals(e.get("stop_reason"))), evs.toString());
            check("S5 已 finished 后 [DONE] 不重复收尾", count(evs, "message_stop", null, null) == 1, evs.toString());
        }

        // S6 sensenova 式：每条内容 chunk 带 "finish_reason":""（空串占位，非 null），真实 stop 与末尾 usage 在其后。
        // 回归锁：空串不得提前收尾（旧实现 fr!=null 会在第一条内容就 message_stop → 客户端"半句就停" + tokens 0+0）。
        {
            StreamState st = new StreamState();
            List<Map<String, Object>> evs1 = feed(st, List.of(
                    tcChunk("\"content\":\"从前\"", ""),
                    tcChunk("\"content\":\"有座山\"", "")));
            check("S6 空串 finish_reason 不提前收尾（未 finished、无 message_stop）",
                    !st.finished && count(evs1, "message_stop", null, null) == 0, evs1.toString());
            List<Map<String, Object>> evs2 = feed(st, List.of(
                    tcChunk("\"content\":\"\"", "stop"),
                    "{\"id\":\"c\",\"choices\":[],\"usage\":{\"prompt_tokens\":22,\"completion_tokens\":78}}"));
            check("S6 真实 stop 才收尾且只 1 个 message_stop",
                    st.finished && count(evs2, "message_stop", null, null) == 1, evs2.toString());
            check("S6 末尾 usage chunk（choices 空）收进 st",
                    st.outputTokens == 78 && Json.longVal(st.usageAcc.get("prompt_tokens")) == 22L,
                    "out=" + st.outputTokens + " acc=" + st.usageAcc);
        }

        System.out.println("\n=== " + (failures == 0 ? "ALL PASS" : failures + " FAILURE(S)") + " ===");
        if (failures > 0) System.exit(1);
    }
}
