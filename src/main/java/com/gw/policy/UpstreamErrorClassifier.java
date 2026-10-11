package com.gw.policy;

/**
 * 上游错误语义分类器（#9）。把"上游返回什么"从字符串模糊匹配升级为稳定枚举，
 * 供三处共用：
 *  1) PolicyService.recordFailure —— 按语义定冷却/豁免策略（文字型限流也触发冷却，
 *     不再因为没带 HTTP 429 就白撞下一轮）；
 *  2) StatsService.logRequest —— 落库 error_class 列，日志页可按类筛选；
 *  3) AdminController —— errorCn 展示文案（原来那 24 条硬编码表搬到这里统一维护）。
 *
 * 设计原则：纯函数、无状态、绝不抛异常；任何无法归类都落到 OTHER（走各自原有兜底分支）。
 * 分类只依赖 (httpStatus, error) 两个输入——上游错误体里已含足够语义，无需解析 JSON。
 */
public final class UpstreamErrorClassifier {

    private UpstreamErrorClassifier() {}

    public enum ErrorClass {
        /** 鉴权失败：Key 无效/过期（401/403）。只该 Key 冷却，不推模型熔断 */
        AUTH,
        /** 配额/余额耗尽：短期不会恢复，Key 长冷却下线 */
        QUOTA,
        /** 限流：TPM/RPM/并发/QPS 超限，含 HTTP 429 与"文字型限流"（HTTP 非 429 但语义是限流）。
         *  按 rateLimitCooldownMs 短冷却 + 自适应学习，不推模型熔断 */
        RATE_LIMIT,
        /** 模型不受支持 / 上游无此模型：短期豁免该家该模型（不碰渠道健康度、不推熔断） */
        MODEL_NOT_SUPPORTED,
        /** 上下文超长 / 参数非法：请求内容问题，与渠道健康无关，只留痕不冷却不熔断 */
        CONTEXT_TOO_LONG,
        /** 该渠道默认开思考、要求把历史 assistant 的 reasoning_content 回传。
         *  网关能治（回填思考 / 关思考重试），不是渠道故障：与 CONTEXT_TOO_LONG 同等处置——
         *  只留痕、不冷却、不推模型熔断。 */
        REASONING_PASSBACK,
        /** 上游过载/内部错误（5xx、gateway overloaded）：计入模型熔断，触发 failover */
        SERVER_BUSY,
        /** 网关侧超时/网络/流中断（首字超时、心跳掐断、TCP 超时、上游提前关闭）：计入熔断 */
        TIMEOUT,
        /** 其余无法归类 */
        OTHER;
    }

    /** 小写化后匹配的关键字表（按语义优先级分组，命中顺序即返回顺序）。
     *  刻意覆盖 logs 实测的"文字型限流"：tpm/rpm limit、1305、50609、concurrency limit、访问量过大。 */
    private static final String[] QUOTA_KEYS = {
            "insufficient_quota", "insufficient balance", "quota exceeded", "余额不足", "exceeded your current quota"};
    private static final String[] RATE_LIMIT_KEYS = {
            "rate_limit_error", "rate limit", "tpm/rpm limit", "request limit[", "requests within",
            "concurrency limit", "too many requests", "throttling", "访问量过大", "限流", "频繁"};
    private static final String[] MODEL_NOT_SUPPORTED_KEYS = {
            "model_not_found", "model not found", "does not exist", "no such model",
            "resolved model is", "not a supported model", "unknown model", "无此模型", "不支持的模型", "模型不存在"};
    private static final String[] CONTEXT_TOO_LONG_KEYS = {
            "context length", "context_length_exceeded", "maximum context", "prompt is too long",
            "too many tokens", "reduce the length", "input is too long", "上下文超", "超过最大长度", "超出上下文"};
    private static final String[] SERVER_BUSY_KEYS = {
            "gateway overloaded", "cache-aware admission", "disk overloaded", "overloaded",
            "service unavailable", "internal server error", "bad gateway", "服务暂不可用", "过载"};
    private static final String[] TIMEOUT_KEYS = {
            "request timed out", "timed out", "timeout", "tcp 连接超时", "连接超时", "首字超时",
            "stream heartbeat timeout", "no real content", "上游提前关闭", "响应体读取超时", "网络错误", "读取失败", "中断"};
    private static final String[] AUTH_KEYS = {
            "unauthorized", "invalid api key", "invalid_api_key", "authentication_error",
            "invalid x-api-key", "apikey", "鉴权失败", "无效的 key"};

    /** 数字错误码 → 限流（部分上游把 429 塞在 200 body 里，只露 code）。 */
    private static boolean isNumericRateLimitCode(String low) {
        return low.contains("429003") || low.contains("1305") || low.contains("50609")
                || low.contains("code 1305") || low.contains("err_code\":1305");
    }

    /**
     * "模型已下线"话术兜底：部分上游（如 AMD）用 HTTP 400 + "Requested model X not supported"、
     * 或 "Model X is not available" 表达"模型不存在"，但既不含 MODEL_NOT_SUPPORTED_KEYS 的固定短语，
     * 状态码又是 400（会被下面兜底误判成 CONTEXT_TOO_LONG）。要求同时出现 "model" 与
     * "not supported / not available" 才判模型不存在——避免把"function calling not supported"这类
     * 特性不支持（不含 model 主语）误伤。
     */
    private static boolean isModelGonePhrasing(String low) {
        if (!low.contains("model")) return false;
        return low.contains("not supported") || low.contains("not_supported")
                || low.contains("not available") || low.contains("not_available");
    }

    /**
     * "思考内容必须回传"话术（DeepSeek 系模型 + 上游默认开思考时）：
     * "The `reasoning_content` in the thinking mode must be passed back to the API."
     * 两个短语同时出现才判——单看 reasoning_content 会误伤"reasoning_content 不能为空"这类其它校验错。
     */
    private static boolean isReasoningPassback(String low) {
        return low.contains("reasoning_content") && low.contains("must be passed back");
    }

    /**
     * 主分类入口。httpStatus 可为 null（如纯文字判定的旧日志行回填）。
     * 先按 error 文本语义判（更具体），再用 HTTP 状态码兜底。
     */
    public static ErrorClass classify(Integer httpStatus, String error) {
        String low = error == null ? "" : error.toLowerCase();
        if (!low.isEmpty()) {
            // 最先判：这类错误状态码是 400，若不先拦会被下面的兜底当成"上下文超长/参数不合法"，
            // 排查时归因就指错方向（实测 trace 里显示成"超时或流中断"）。
            if (isReasoningPassback(low)) return ErrorClass.REASONING_PASSBACK;
            if (containsAny(low, QUOTA_KEYS)) return ErrorClass.QUOTA;
            if (containsAny(low, RATE_LIMIT_KEYS) || isNumericRateLimitCode(low)) return ErrorClass.RATE_LIMIT;
            if (containsAny(low, MODEL_NOT_SUPPORTED_KEYS) || isModelGonePhrasing(low)) return ErrorClass.MODEL_NOT_SUPPORTED;
            if (containsAny(low, CONTEXT_TOO_LONG_KEYS)) return ErrorClass.CONTEXT_TOO_LONG;
            if (containsAny(low, AUTH_KEYS) && httpStatus == null) return ErrorClass.AUTH;
            if (containsAny(low, SERVER_BUSY_KEYS)) return ErrorClass.SERVER_BUSY;
            if (containsAny(low, TIMEOUT_KEYS)) return ErrorClass.TIMEOUT;
        }
        int s = httpStatus == null ? 0 : httpStatus;
        if (s == 429) return ErrorClass.RATE_LIMIT;
        if (s == 401 || s == 403) return ErrorClass.AUTH;
        if (s == 404) return ErrorClass.MODEL_NOT_SUPPORTED;
        if (s == 400 || s == 422) return ErrorClass.CONTEXT_TOO_LONG;
        if (s >= 500) return ErrorClass.SERVER_BUSY;
        if (s == 0 && !low.isEmpty()) return ErrorClass.TIMEOUT;  // 无状态码但有错→多为超时/网络
        return ErrorClass.OTHER;
    }

    public static ErrorClass classify(int httpStatus, String error) {
        return classify(httpStatus == 0 ? null : Integer.valueOf(httpStatus), error);
    }

    /** 展示文案（管理页错误列中文摘要）：命中分类给一句人话，OTHER/无错返回 null（前端显示原文）。 */
    public static String displayCn(ErrorClass ec, String raw) {
        switch (ec) {
            case QUOTA: return "配额耗尽（免费额度或余额用完）";
            case RATE_LIMIT: return "触发限流（TPM/RPM/并发/QPS 超限）";
            case AUTH: return "鉴权失败（API Key 无效或过期）";
            case MODEL_NOT_SUPPORTED: return "模型不受支持（上游无此模型）";
            case CONTEXT_TOO_LONG: return "上下文超长/参数不合法";
            case REASONING_PASSBACK: return "该渠道默认开思考，要求历史回传 reasoning_content（思考内容）";
            case SERVER_BUSY: return "上游过载或内部错误";
            case TIMEOUT: return "超时或流中断（首字/心跳/网络）";
            case OTHER:
            default:
                if (raw == null || raw.isBlank()) return null;
                return null;  // 未归类：前端回退显示原文
        }
    }

    private static boolean containsAny(String low, String[] keys) {
        for (String k : keys) if (low.contains(k.toLowerCase())) return true;
        return false;
    }
}
