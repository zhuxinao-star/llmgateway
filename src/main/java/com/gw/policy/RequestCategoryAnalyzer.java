package com.gw.policy;

import com.gw.provider.adaptor.Json;

import java.util.List;
import java.util.Map;/**
 * 请求语义分析（轻量关键词规则，零额外 token 成本）：
 * 从最后一条 user 消息的文本内容判定请求类别，供策略层做语义路由——
 * 擅长该类别的模型（routing 条目的 category 标签）在评分中获得加成。
 *
 * 类别与 routing 条目 category 字段对应：code / doc / translate / analysis / general。
 * 判不了就返回 null（不影响排序，全部按通用逻辑走）。
 */
public final class RequestCategoryAnalyzer {

    private RequestCategoryAnalyzer() {}

    /**
     * 关键词权重表：命中一个词得相应分，类别按总分最高者。
     * 中英文混合（Agent 工具常发英文指令、中文闲聊也常见）。
     */
    private static final List<String> CODE_KEYS = List.of(
            "代码", "函数", "编译", "报错", "调试", "重构", "接口", "变量", "脚本", "bug", "Bug", "BUG",
            "code", "Code", "function", "compile", "error", "debug", "refactor", "API", "stack trace",
            "exception", "Exception", " NullPointerException", "syntax", "merge", "commit", "git ",
            "class ", "def ", "import ", "SELECT ", "UPDATE ", "INSERT ", "regression", "单元测试",
            "单元 test", "lint", "type", "变量名", "正则"
    );

    private static final List<String> DOC_KEYS = List.of(
            "文档", "总结", "摘要", "报告", "说明", "方案", "大纲", "文章", "周报", "日报", "简历", "word", "word文档",
            "写一篇", "写一份", "拟一份", "起草", "documentation", "summary", "summarize", "outline", "report",
            "draft", "essay", "README", "meeting notes", "会议纪要"
    );

    private static final List<String> TRANSLATE_KEYS = List.of(
            "翻译", "译成", "translated", "translate", "translation", "日语", "英语", "法语", "德语", "韩语",
            "英文的", "中文的", "翻一下"
    );

    private static final List<String> ANALYSIS_KEYS = List.of(
            "分析", "原因", "为什么", "怎么回事", "根因", "对比", "比较", "优缺点", "权衡", "evaluate", "why ",
            "analyze", "analysis", "root cause", "compare", "trade-off", "tradeoff", "利弊", "可行性", "原理"
    );

    /** 从 OpenAI 格式请求体判定语义类别；判不了返回 null */
    public static String analyze(Map<String, Object> openAiRequest) {
        String text = lastUserText(openAiRequest);
        if (text == null || text.isBlank()) return null;
        String t = text;
        int code = hits(t, CODE_KEYS);
        // code 判定更严格：命中>=2 或含代码块特征才判 code（单个词如"type"太宽）
        boolean codeHit = code >= 2 || t.contains("```") || t.contains("Stack trace")
                || t.contains("Traceback") || t.contains("at com.") || t.contains("at java.")
                || t.contains(".java:") || t.contains(".py:") || t.contains(".ts:") || t.contains(".go:")
                || t.contains("Exception") || t.contains("错误码") || t.contains("退出码");
        int doc = hits(t, DOC_KEYS);
        int translate = hits(t, TRANSLATE_KEYS);
        int analysis = hits(t, ANALYSIS_KEYS);

        if (codeHit) return "code";
        if (translate >= 1 && translate >= doc && translate >= analysis) return "translate";
        if (doc >= 2 && doc >= analysis) return "doc";
        if (analysis >= 2) return "analysis";
        return null;  // 闲聊/普通问答：不偏移排序
    }

    /** 粗估请求输入 token 数。用于 contextWindow 预过滤——宁可粗一点也别把超限请求送上小窗口模型。
     *  估算口径（经验值，偏保守）：
     *  - 中文/全角字符 ≈ 0.7~1 token/字，按 2 字符≈1 token（原 /4 会低估 3~4 倍，中文超长请求漏拦）；
     *  - 英文/ASCII ≈ 4 字符≈1 token；
     *  - base64 图片不按字符长度计（137K 字符会高估到 3.4 万 token，误触压缩且丢图），
     *    按视觉模型一图固定 ~1000 token 计。
     *  多模态端点（embeddings 的 input / images 的 prompt）无 messages，返回 0 由调用方另行处理。 */
    public static int estimateInputTokens(Map<String, Object> openAiRequest) {
        List<Object> messages = Json.asList(openAiRequest.get("messages"));
        if (messages == null) return 0;
        int[] counter = new int[3];   // [0]=ASCII 字符数, [1]=CJK 字符数, [2]=图片固定 token
        for (Object o : messages) accumulateMessage(Json.asMap(o), counter);
        // tools 定义（函数名/描述/参数 JSON Schema）同样占用输入：Agent 工具动辄几十把、
        // schema 合计常达数万 token。此前只算 messages 会系统性低估（实测 70K 请求估成
        // 阈值以下 → 压缩不触发、超限请求打到小窗口模型 400）。
        Object tools = openAiRequest.get("tools");
        if (tools != null) accumulateText(Json.write(tools), counter);
        return counter[0] / 4 + counter[1] / 2 + counter[2];
    }

    /** 单条消息 token 估算（压缩保留段按 token 预算裁剪用）。含 content 各段、
     *  tool_calls 参数；每条再加 ~4 token 的角色/结构开销。 */
    public static int estimateMessageTokens(Map<String, Object> msg) {
        int[] counter = new int[3];
        accumulateMessage(msg, counter);
        return 4 + counter[0] / 4 + counter[1] / 2 + counter[2];
    }

    /** 累加单条消息的全部文本输入（与 estimateInputTokens 原循环体同口径） */
    private static void accumulateMessage(Map<String, Object> m, int[] counter) {
        if (m == null) return;
        Object content = m.get("content");
        if (content instanceof List<?> parts) {
            for (Object p : parts) {
                Map<String, Object> part = Json.asMap(p);
                if (part == null) continue;
                if (part.get("text") instanceof String s) accumulateText(s, counter);
                if (part.get("image_url") != null) {
                    Map<String, Object> iu = Json.asMap(part.get("image_url"));
                    if (iu != null && iu.get("url") instanceof String u && !u.isBlank()) counter[2] += IMAGE_TOKEN_COST;
                }
            }
        } else if (content instanceof String s) {
            accumulateText(s, counter);
        }
        // tool_calls 的函数参数 JSON 也算输入
        List<Object> calls = Json.asList(m.get("tool_calls"));
        if (calls != null) {
            for (Object call : calls) {
                Map<String, Object> cm = Json.asMap(call);
                if (cm == null) continue;
                Object fn = cm.get("function");
                Map<String, Object> fm = Json.asMap(fn);
                if (fm != null && fm.get("arguments") instanceof String s) accumulateText(s, counter);
            }
        }
    }

    /** 请求是否带工具定义（tools 非空数组）——工具弱模型降权用 */
    public static boolean requiresTools(Map<String, Object> openAiRequest) {
        List<Object> tools = Json.asList(openAiRequest.get("tools"));
        return tools != null && !tools.isEmpty();
    }

    /** 视觉模型一张图的固定 token 估算（1024×1024 常见模型约 600~1500，取 1000 偏保守） */
    private static final int IMAGE_TOKEN_COST = 1000;

    /** 逐字符分类累加：ASCII → counter[0]，CJK/全角 → counter[1] */
    private static void accumulateText(String s, int[] counter) {
        if (s == null || s.isEmpty()) return;
        for (int i = 0; i < s.length(); i++) {   // 按 code point 遍历更安全，但 message 文本极端 unicode 罕见，char 遍历够用
            char c = s.charAt(i);
            if (isCjk(c)) counter[1]++; else counter[0]++;
        }
    }

    /** 判中日韩统一表意文字及全角符号/假名（这些在 BPE 里 token 密度接近中文） */
    private static boolean isCjk(char c) {
        return (c >= '\u4e00' && c <= '\u9fff')      // CJK 统一表意文字
                || (c >= '\u3040' && c <= '\u30ff')   // 日文假名
                || (c >= '\uac00' && c <= '\ud7af')   // 韩文音节
                || (c >= '\uf900' && c <= '\ufaff')   // CJK 兼容表意文字
                || (c >= '\uff00' && c <= '\uffef');  // 全角形式
    }

    /** 取最后一条 user 消息的文本内容（content 可能是 string 或分段数组） */
    private static String lastUserText(Map<String, Object> req) {
        List<Object> messages = Json.asList(req.get("messages"));
        if (messages == null) return null;
        for (int i = messages.size() - 1; i >= 0; i--) {
            Map<String, Object> m = Json.asMap(messages.get(i));
            if (m == null) continue;
            if (!"user".equals(Json.str(m.get("role")))) continue;
            return contentToText(m.get("content"));
        }
        // 没有 user 消息：退而取最后一条任意角色
        Map<String, Object> last = Json.asMap(messages.get(messages.size() - 1));
        return last == null ? null : contentToText(last.get("content"));
    }

    private static String contentToText(Object content) {
        if (content instanceof String s) return s;
        if (content instanceof List<?> parts) {
            StringBuilder sb = new StringBuilder();
            for (Object o : parts) {
                Map<String, Object> p = Json.asMap(o);
                if (p == null) continue;
                Object text = p.get("text");
                if (text instanceof String s) sb.append(s).append('\n');
            }
            return sb.toString();
        }
        return null;
    }

    private static int hits(String text, List<String> keys) {
        int n = 0;
        for (String k : keys) if (text.contains(k)) n++;
        return n;
    }
}
