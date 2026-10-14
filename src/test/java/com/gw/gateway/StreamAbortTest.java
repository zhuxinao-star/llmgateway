package com.gw.gateway;

import com.gw.provider.adaptor.AnthropicConverter;
import com.gw.provider.adaptor.AnthropicConverter.StreamState;
import com.gw.provider.adaptor.Json;

import java.util.*;

/**
 * 快速单测（main 直跑）：网关掐断一条"已提交 200 的流"时的收尾帧（2026-10-14 早停事故回归锚）。
 * 事故：心跳超时把流掐断后一个字节都没写，Claude Code 收到没有 message_stop 的半截流，
 * 把"中断"读成"这轮答完了"→ 任务没完成就静默断开。修复=AnthropicConverter.abortFrames。
 * 红线（断言逐条盯住）：
 *   ① 开过的每个 content block 都必须有配对的 content_block_stop（否则客户端卡在"块未闭合"）；
 *   ② 最后一帧必须是 event: error（这是唯一能让客户端明确报错/重试的信号通道）；
 *   ③ 绝不允许出现 message_stop / stop_reason——那等于谎报"答完了"；
 *   ④ 帧必须是单 \n 结尾（写出方补空行成 SSE 事件分隔，双 \n 会变成空事件）；
 *   ⑤ 掐断后上游才姗姗来迟地发 [DONE]，不得再补发一套收尾（finished 已置位）。
 */
public class StreamAbortTest {

    static int failures = 0;

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    /** 某个事件类型的帧数（注意不能数子串：一帧里 "content_block_stop" 会出现两次——event 行与 data 行） */
    static int nFrames(List<String> frames, String eventType) {
        int n = 0;
        for (String f : frames) if (f.startsWith("event: " + eventType)) n++;
        return n;
    }

    /** 喂一批上游 data: 行，产出 Anthropic 事件帧（保持原始字符串，好校验帧边界） */
    static List<String> feed(StreamState st, List<String> chunks) {
        List<String> out = new ArrayList<>();
        for (String c : chunks) {
            for (String e : AnthropicConverter.openaiChunkToAnthropicEvents("data: " + c, st, "m")) {
                out.add(e);
            }
        }
        return out;
    }

    static String textChunk(String s) {
        return "{\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + s + "\"}}]}";
    }

    static String toolChunk(String id, int idx, String name, String argsJson) {
        // arguments 在 OpenAI 协议里是"字符串"，所以里面的引号必须再转义一层，
        // 否则整条 chunk 不是合法 JSON，转换器一帧都不产（测试会假通过成"没有块要关"）
        String esc = argsJson.replace("\\", "\\\\").replace("\"", "\\\"");
        return "{\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{"
                + "\"index\":" + idx + ",\"id\":\"" + id + "\",\"type\":\"function\","
                + "\"function\":{\"name\":\"" + name + "\",\"arguments\":\"" + esc + "\"}}]}}]}";
    }

    /** 事件帧里出现的 block index 集合（start / stop 各取一次，用于"开过的块必须都关掉"对照） */
    static Set<Long> idxOf(List<String> frames, String eventType) {
        Set<Long> s = new LinkedHashSet<>();
        for (String f : frames) {
            if (!f.startsWith("event: " + eventType)) continue;
            Map<String, Object> ev = Json.parseObject(f.substring(f.indexOf("data:") + 5).strip());
            Long i = Json.longVal(ev.get("index"));
            if (i != null) s.add(i);
        }
        return s;
    }

    /** 把事件帧按写出方（AnthropicController.abortStream）的补空行规则拼成字节流文本 */
    static String wire(List<String> frames) {
        StringBuilder sb = new StringBuilder();
        for (String f : frames) sb.append(f.endsWith("\n\n") ? f : f + "\n");
        return sb.toString();
    }

    static List<String> abort(StreamState st, String msg) {
        return AnthropicConverter.abortFrames(st, msg);
    }

    public static void main(String[] args) throws Exception {
        String MSG = "网关静默 30000ms 未收到上游新内容，已中断本次流式响应（本轮未完成，请重试）";

        // ---- T1 文本块进行中掐断 ----
        {
            StreamState st = new StreamState();
            List<String> sent = feed(st, List.of(textChunk("你好"), textChunk("世界")));
            check("T1 前置：喂进内容会开 text 块", !idxOf(sent, "content_block_start").isEmpty(),
                    String.valueOf(sent));
            check("T1 前置：正常流不置 finished", !st.finished, "finished=true 说明喂流就收尾了");
            List<String> fr = abort(st, MSG);
            String all = String.join("|", fr);
            check("T1 开过的块全部有配对 content_block_stop",
                    idxOf(sent, "content_block_start").equals(idxOf(fr, "content_block_stop"))
                            && !idxOf(fr, "content_block_stop").isEmpty(),
                    "start=" + idxOf(sent, "content_block_start") + " stop=" + idxOf(fr, "content_block_stop"));
            check("T1 最后一帧是 error", fr.get(fr.size() - 1).startsWith("event: error"),
                    fr.get(fr.size() - 1));
            check("T1 无 message_stop（不许谎报答完）", !all.contains("message_stop"), all);
            check("T1 无 stop_reason", !all.contains("stop_reason"), all);
            check("T1 掐断后 finished=true（防重复收尾）", st.finished, "false");
            check("T1 中文消息不被转义坏", fr.get(fr.size() - 1).contains("本轮未完成"),
                    fr.get(fr.size() - 1));
        }

        // ---- T2 并行两工具块都未闭合时掐断 ----
        {
            StreamState st = new StreamState();
            List<String> sent = feed(st, List.of(toolChunk("tc_1", 0, "read_file", "{\"path\":\"a\"}"),
                    toolChunk("tc_2", 1, "write_file", "{\"path\":\"b\"}")));
            Set<Long> opened = idxOf(sent, "content_block_start");
            check("T2 前置：两个工具块都开了独立下标", opened.size() == 2, String.valueOf(opened));
            List<String> fr = abort(st, MSG);
            String all = String.join("|", fr);
            Set<Long> closed = idxOf(fr, "content_block_stop");
            check("T2 开过的块全部有配对 content_block_stop", opened.equals(closed),
                    "start=" + opened + " stop=" + closed);
            check("T2 恰好两帧 stop（不多关一次）",
                    nFrames(fr, "content_block_stop") == 2 && closed.size() == 2,
                    "frames=" + nFrames(fr, "content_block_stop") + " idx=" + closed);
            check("T2 error 仍在最后", fr.get(fr.size() - 1).startsWith("event: error"),
                    fr.get(fr.size() - 1));
            check("T2 无 message_stop", !all.contains("message_stop"), all);
        }

        // ---- T3 还没吐出任何块就掐断（首字超时） ----
        {
            StreamState st = new StreamState();
            List<String> fr = abort(st, MSG);
            check("T3 只发 error 一帧", fr.size() == 1 && fr.get(0).startsWith("event: error"),
                    String.join("|", fr));
            check("T3 不凭空关块", idxOf(fr, "content_block_stop").isEmpty(), String.join("|", fr));
        }

        // ---- T4 帧边界与 error 载荷结构 ----
        {
            StreamState st = new StreamState();
            feed(st, List.of(textChunk("x")));
            String wire = wire(abort(st, MSG));
            check("T4 事件以空行分隔", wire.contains("\n\n") && !wire.contains("\n\n\n"),
                    wire.replace("\n", "\\n"));
            check("T4 末尾留一个事件分隔空行", wire.endsWith("\n\n"), wire.replace("\n", "\\n"));
            for (String f : (String[]) wire.split("\n\n", -1)) {
                if (f.isBlank()) continue;
                check("T4 每帧含 event: 与 data: 两段",
                        f.startsWith("event: ") && f.contains("\ndata: "), f.replace("\n", "\\n"));
            }
            String errFrame = wire.substring(wire.lastIndexOf("event: error"));
            Map<String, Object> ev = Json.parseObject(
                    errFrame.substring(errFrame.indexOf("data:") + 5).strip().replaceAll("\\n+$", ""));
            check("T4 error 帧 type=error", "error".equals(Json.str(ev.get("type"))), String.valueOf(ev));
            Map<String, Object> inner = Json.asMap(ev.get("error"));
            check("T4 error.error.type=api_error", "api_error".equals(Json.str(inner.get("type"))),
                    String.valueOf(inner));
            check("T4 error.message 原文完整", MSG.equals(Json.str(inner.get("message"))),
                    String.valueOf(inner.get("message")));
        }

        // ---- T5 掐断后上游才发 [DONE]：不得二次收尾 ----
        {
            StreamState st = new StreamState();
            feed(st, List.of(textChunk("x")));
            abort(st, MSG);
            List<String> late = AnthropicConverter.openaiChunkToAnthropicEvents("data: [DONE]", st, "m");
            check("T5 [DONE] 后补的收尾被丢弃", late.isEmpty(), String.join("|", late));
        }

        System.out.println();
        if (failures == 0) {
            System.out.println("=== ALL PASS ===");
        } else {
            System.out.println("=== " + failures + " FAILURES ===");
            System.exit(1);
        }
    }
}
