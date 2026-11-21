package com.gw.policy;

/**
 * 快速单测（main 运行）：分类器新增话术（2026-11-18 批）。
 * "Model X was disabled"（400）必须归 MODEL_NOT_SUPPORTED（豁免冷却，不再每轮白撞）；
 * "apikey status is overdue" 必须归 QUOTA（Key 长冷却）。错误体取自 2026-11-17 request_log 真实原文。
 */
public class ClassifierGuardTest {

    static int fails = 0;

    static void check(String name, UpstreamErrorClassifier.ErrorClass got, UpstreamErrorClassifier.ErrorClass want, String detail) {
        boolean ok = got == want;
        System.out.printf("%s %s got=%s%s%n", ok ? "PASS" : "FAIL", name, got, detail == null ? "" : "  ← " + detail);
        if (!ok) fails++;
    }

    public static void main(String[] args) {
        // 2026-11-17 真实错误体：禁用模型（HTTP 400）此前落"只留痕"档，被反复白撞 5 次
        String disabled = "{\"error\":{\"message\":\"Model 'deepseek-v4-flash-vision' was disabled on "
                + "the provider.\",\"type\":\"invalid_request_error\"}}";
        check("禁用模型→MODEL_NOT_SUPPORTED",
                UpstreamErrorClassifier.classify(400, disabled),
                UpstreamErrorClassifier.ErrorClass.MODEL_NOT_SUPPORTED, null);

        // 余额过期 key（此前靠 insufficient balance 命中，overdue 单独出现也要命中）
        check("余额过期→QUOTA",
                UpstreamErrorClassifier.classify(400, "{\"error\":{\"message\":\"apikey status is overdue\"}}"),
                UpstreamErrorClassifier.ErrorClass.QUOTA, null);
        check("余额不足(老口径)→QUOTA",
                UpstreamErrorClassifier.classify(400, "{\"error\":{\"message\":\"Insufficient balance: apikey status is overdue\"}}"),
                UpstreamErrorClassifier.ErrorClass.QUOTA, null);

        // 已下线中文话术
        check("中文下线→MODEL_NOT_SUPPORTED",
                UpstreamErrorClassifier.classify(400, "该模型已下线：模型已停用"),
                UpstreamErrorClassifier.ErrorClass.MODEL_NOT_SUPPORTED, null);

        // 回归：非 model 主语的特性不支持不误伤（2026-11-17 前就有的口径）
        check("特性不支持不误伤",
                UpstreamErrorClassifier.classify(400, "function calling not supported by this endpoint"),
                UpstreamErrorClassifier.ErrorClass.CONTEXT_TOO_LONG, null);

        // 2026-11-19 批：isContextLengthError（窗口学习的前置判定，只认文本语义）
        boolean zhipu = UpstreamErrorClassifier.isContextLengthError("{\"error\":{\"message\":\"Prompt exceeds max length\"}}");
        boolean mt = UpstreamErrorClassifier.isContextLengthError("{\"error\":{\"message\":\"max_tokens 不能超过 65536\"}}");
        boolean cl = UpstreamErrorClassifier.isContextLengthError("This model's maximum context length is 8192 tokens");
        boolean okCtx = zhipu && !mt && cl;
        System.out.printf("%s 超长可学/max_tokens 不学/窗口话术可学  zhipu=%s max_tokens=%s context=%s%n",
                okCtx ? "PASS" : "FAIL", zhipu, mt, cl);
        if (!okCtx) fails++;

        System.out.println(fails == 0 ? "\nALL PASS" : "\nFAILED: " + fails);
        if (fails > 0) System.exit(1);
    }
}
