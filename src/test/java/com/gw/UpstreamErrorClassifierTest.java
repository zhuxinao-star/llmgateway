package com.gw;

import com.gw.policy.UpstreamErrorClassifier;
import com.gw.policy.UpstreamErrorClassifier.ErrorClass;

/**
 * 快速单测（main 直跑）：#9 UpstreamErrorClassifier 语义分类回归。
 * 核心验证——"文字型限流"（不带 HTTP 429）必须被判成 RATE_LIMIT，
 * 这是老逻辑的漏网点（会白撞熔断、下个请求继续撞同一面墙）。
 * 同时覆盖 QUOTA 优先级、MODEL_NOT_SUPPORTED、AUTH、CONTEXT、5xx、超时、OTHER 兜底，
 * 以及展示层 displayCn（translateError 已委托它）。
 */
public class UpstreamErrorClassifierTest {

    static int failures = 0;

    static void check(String name, Object expected, Object actual) {
        boolean ok = expected == null ? actual == null : expected.equals(actual);
        System.out.println((ok ? "PASS " : "FAIL ") + name + (ok ? "" : "  期望=" + expected + " 实际=" + actual));
        if (!ok) failures++;
    }

    public static void main(String[] args) {
        // 1. HTTP 状态码路径
        check("429 -> RATE_LIMIT", ErrorClass.RATE_LIMIT, UpstreamErrorClassifier.classify(429, null));
        check("401 -> AUTH", ErrorClass.AUTH, UpstreamErrorClassifier.classify(401, null));
        check("403 -> AUTH", ErrorClass.AUTH, UpstreamErrorClassifier.classify(403, null));
        check("404 -> MODEL_NOT_SUPPORTED", ErrorClass.MODEL_NOT_SUPPORTED, UpstreamErrorClassifier.classify(404, null));
        check("500 -> SERVER_BUSY", ErrorClass.SERVER_BUSY, UpstreamErrorClassifier.classify(500, null));
        check("503 -> SERVER_BUSY", ErrorClass.SERVER_BUSY, UpstreamErrorClassifier.classify(503, "Service Unavailable"));

        // 2. 文字型限流（老逻辑漏网点：HTTP 非 429 但语义是限流）
        check("tpm/rpm 文字限流(status0)", ErrorClass.RATE_LIMIT, UpstreamErrorClassifier.classify(0, "exceeds tpm/rpm limit"));
        check("1305 访问量过大(200 body)", ErrorClass.RATE_LIMIT, UpstreamErrorClassifier.classify(200, "{\"code\":1305,\"msg\":\"访问量过大\"}"));
        check("concurrency limit(500)", ErrorClass.RATE_LIMIT, UpstreamErrorClassifier.classify(500, "reach concurrency limit (64)"));
        check("Maximum 8 requests", ErrorClass.RATE_LIMIT, UpstreamErrorClassifier.classify(0, "Maximum 8 requests within 1 minutes"));
        check("rate_limit_error", ErrorClass.RATE_LIMIT, UpstreamErrorClassifier.classify(0, "rate_limit_error"));
        check("429003", ErrorClass.RATE_LIMIT, UpstreamErrorClassifier.classify(200, "error code 429003"));

        // 3. QUOTA 优先于 RATE_LIMIT（insufficient_quota 含 quota 语义要判 QUOTA）
        check("429+insufficient_quota -> QUOTA", ErrorClass.QUOTA, UpstreamErrorClassifier.classify(429, "insufficient_quota"));
        check("Insufficient balance -> QUOTA", ErrorClass.QUOTA, UpstreamErrorClassifier.classify(400, "Insufficient balance"));

        // 4. 模型不支持
        check("model_not_found", ErrorClass.MODEL_NOT_SUPPORTED, UpstreamErrorClassifier.classify(400, "The model `gpx` does not exist"));
        check("resolved model is", ErrorClass.MODEL_NOT_SUPPORTED, UpstreamErrorClassifier.classify(502, "no resolved model is available"));
        // AMD 上游已下架：HTTP 400 "not supported" 不能被误判成 CONTEXT_TOO_LONG
        check("AMD 400 not supported", ErrorClass.MODEL_NOT_SUPPORTED, UpstreamErrorClassifier.classify(400, "Requested model DeepSeek-V4-Flash not supported"));
        check("404 is not available", ErrorClass.MODEL_NOT_SUPPORTED, UpstreamErrorClassifier.classify(404, "Model qwen3.8-flash-next is not available"));
        check("400 is not available", ErrorClass.MODEL_NOT_SUPPORTED, UpstreamErrorClassifier.classify(400, "Model GLM-5.3-Flash is not available"));
        // 不误伤：无 "model" 主语的 "not supported" 仍归 OTHER，不抢模型语义
        check("function calling not supported 不误判", true,
                UpstreamErrorClassifier.classify(400, "function calling is not supported") != ErrorClass.MODEL_NOT_SUPPORTED);

        // 5. 上下文超长
        check("context length", ErrorClass.CONTEXT_TOO_LONG, UpstreamErrorClassifier.classify(400, "maximum context length is 32768"));
        check("prompt too long", ErrorClass.CONTEXT_TOO_LONG, UpstreamErrorClassifier.classify(400, "Your prompt is too long"));

        // 5b. 思考内容回传（实测生产错误原文：lingxiongkeji/deepseek-v4-* 默认开思考 + 历史工具轮没带 reasoning_content）
        String passback = "{\"error\":{\"message\":\"The `reasoning_content` in the thinking mode must be "
                + "passed back to the API. (request_id: 9c8d433f-e74d-4cb1-923f-11365a4dea20)\","
                + "\"type\":\"invalid_request_error\",\"param\":\"\",\"code\":\"invalid_request_error\"}}";
        check("reasoning 回传(400)", ErrorClass.REASONING_PASSBACK, UpstreamErrorClassifier.classify(400, passback));
        // trace 的 errorCn 走 classify(null, raw)：不带状态码也必须判对，否则又显示成"超时或流中断"
        check("reasoning 回传(无状态码)", ErrorClass.REASONING_PASSBACK, UpstreamErrorClassifier.classify(null, passback));
        check("reasoning 回传不被兜底抢判", true,
                UpstreamErrorClassifier.classify(400, passback) != ErrorClass.CONTEXT_TOO_LONG);
        // 不误伤：只提 reasoning_content、没有"must be passed back"的其它校验错仍走原兜底
        check("reasoning_content 为空 不误判", true,
                UpstreamErrorClassifier.classify(400, "reasoning_content must not be empty")
                        != ErrorClass.REASONING_PASSBACK);
        check("displayCn(REASONING_PASSBACK) 非空", true,
                UpstreamErrorClassifier.displayCn(ErrorClass.REASONING_PASSBACK, "x") != null);

        // 6. 鉴权文字（无状态码）
        check("Unauthorized text", ErrorClass.AUTH, UpstreamErrorClassifier.classify(null, "Unauthorized: invalid api key"));

        // 7. 超时（status0 + 超时文字）
        check("首字超时", ErrorClass.TIMEOUT, UpstreamErrorClassifier.classify(0, "首字超时(>3000ms)"));
        check("stream heartbeat timeout", ErrorClass.TIMEOUT, UpstreamErrorClassifier.classify(0, "stream heartbeat timeout"));
        check("响应体读取超时", ErrorClass.TIMEOUT, UpstreamErrorClassifier.classify(0, "响应体读取超时(>120ms)"));

        // 8. 上游过载
        check("gateway overloaded", ErrorClass.SERVER_BUSY, UpstreamErrorClassifier.classify(500, "gateway overloaded"));
        check("cache-aware admission", ErrorClass.SERVER_BUSY, UpstreamErrorClassifier.classify(503, "cache-aware admission unavailable"));

        // 9. OTHER 兜底
        check("空错误+无状态 -> OTHER", ErrorClass.OTHER, UpstreamErrorClassifier.classify(null, null));
        check("未知文字+200 -> OTHER", ErrorClass.OTHER, UpstreamErrorClassifier.classify(200, "some totally unknown thing happened"));

        // 10. 展示层：命中的给文案，OTHER 给 null（前端回退原文）
        check("displayCn(QUOTA)", "配额耗尽（免费额度或余额用完）", UpstreamErrorClassifier.displayCn(ErrorClass.QUOTA, "x"));
        check("displayCn(OTHER) -> null", null, UpstreamErrorClassifier.displayCn(ErrorClass.OTHER, "some unknown"));
        check("displayCn(RATE_LIMIT) 非空", true,
                UpstreamErrorClassifier.displayCn(ErrorClass.RATE_LIMIT, "x") != null);

        System.out.println();
        if (failures == 0) {
            System.out.println("=== ALL PASS ===");
        } else {
            System.out.println("=== " + failures + " FAILURES ===");
            System.exit(1);
        }
    }
}
