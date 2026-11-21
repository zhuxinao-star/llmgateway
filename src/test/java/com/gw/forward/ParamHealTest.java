package com.gw.forward;

import java.util.List;
import java.util.Map;

/**
 * 快速单测（main 运行）：候选级参数自愈（2026-11-18 批）。
 * 错误体样本全部取自 2026-11-17 request_log 真实原文。
 */
public class ParamHealTest {

    static int fails = 0;

    static void check(boolean ok, String name, String detail) {
        System.out.printf("%s %s%s%n", ok ? "PASS" : "FAIL", name, detail == null ? "" : "  ← " + detail);
        if (!ok) fails++;
    }

    public static void main(String[] args) {
        // ---- reasoning_effort 组合冲突（真实错误体）----
        String conflict = "{\"error\":{\"message\":\"Cannot specify both reasoning_effort and reasoning.effort. "
                + "Use one or the other.\",\"type\":\"invalid_request_error\",\"code\":\"invalid_request\"}}";
        List<String> strip = ParamHealer.conflictParamsToStrip(conflict);
        check(strip.size() == 1 && strip.contains("reasoning_effort"),
                "组合冲突→剥 reasoning_effort", String.valueOf(strip));
        check(ParamHealer.detectHeal(conflict) != null
                        && ParamHealer.detectHeal(conflict).strip().contains("reasoning_effort"),
                "detectHeal 识别组合冲突", null);
        check(ParamHealer.conflictParamsToStrip("普通错误信息").isEmpty(), "普通错误不误剥", null);
        check(ParamHealer.conflictParamsToStrip(null).isEmpty(), "null 错误体安全", null);
        // 无主语误伤防护：只提到 reasoning_content 回传的不算组合冲突
        String passback = "The `reasoning_content` in the thinking mode must be passed back to the API.";
        check(ParamHealer.conflictParamsToStrip(passback).isEmpty(), "思考回传错误不误判", null);

        // ---- max_tokens 上限（真实错误体）----
        String agnes = "{\"error\":{\"message\":\"max_tokens 不能超过 65536 (request id: 20261117...)\","
                + "\"type\":\"AgnesAI_error\",\"param\":\"\",\"code\":\"invalid_request\"}}";
        int cap = ParamHealer.parseMaxTokensCap(agnes);
        check(cap == 65536, "中文话术解析上限", String.valueOf(cap));
        String en = "{\"error\":{\"message\":\"max_tokens is too large: 1000000 exceeds the maximum of 65536\"}}";
        check(ParamHealer.parseMaxTokensCap(en) == 65536, "英文话术解析上限", String.valueOf(ParamHealer.parseMaxTokensCap(en)));
        check(ParamHealer.detectHeal(agnes) != null && ParamHealer.detectHeal(agnes).cap() == 65536,
                "detectHeal 识别上限", null);
        check(ParamHealer.parseMaxTokensCap("context length exceeded") == -1, "无关错误不解析出上限", null);

        // ---- 学习 + 预适配（实例方法，防御性副本）----
        ParamHealer ph = new ParamHealer();
        Map<String, Object> req = new java.util.LinkedHashMap<>();
        req.put("model", "deepseek-v4-flash");
        req.put("reasoning_effort", "low");
        req.put("max_tokens", 100000);
        Map<String, Object> healed = ph.learnAndApply("agnes", "agnes-3.0-flash", req,
                new ParamHealer.Heal(List.of("reasoning_effort"), 65536));
        check(!healed.containsKey("reasoning_effort"), "学习后副本剥参数", String.valueOf(healed.keySet()));
        Object mt = healed.get("max_tokens");
        check(mt instanceof Integer i && i == 65536, "学习后副本钳 max_tokens", String.valueOf(mt));
        check(req.containsKey("reasoning_effort") && req.get("max_tokens").equals(100000),
                "调用方原 map 不被改写", null);
        // 预适配：学习过的家自动剥/钳；没学过的家原样
        Map<String, Object> pre = ph.preAdapt("agnes", "agnes-3.0-flash", req);
        check(!pre.containsKey("reasoning_effort") && ((Integer) pre.get("max_tokens")) == 65536,
                "预适配对学过的家生效", null);
        Map<String, Object> raw = ph.preAdapt("other", "other-model", req);
        check(raw.containsKey("reasoning_effort") && raw.get("max_tokens").equals(100000),
                "预适配对没学过的家原样", null);

        System.out.println(fails == 0 ? "\nALL PASS" : "\nFAILED: " + fails);
        if (fails > 0) System.exit(1);
    }
}
