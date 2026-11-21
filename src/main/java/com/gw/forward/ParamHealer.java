package com.gw.forward;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 候选级参数自愈（2026-11-17 request_log 实测驱动，两类同形失败各烧了十几次重试）：
 *
 * 1. 组合冲突：客户端同时带顶层 reasoning_effort 与结构化 reasoning.effort，部分上游
 *    400 "Cannot specify both reasoning_effort and reasoning.effort. Use one or the other."
 *    → 学会按 provider|model 剥离顶层 reasoning_effort（保留结构化对象），此后预剥离零浪费；
 * 2. 输出上限：agnes 等家有平台硬上限（max_tokens 不能超过 65536），请求值超限必 400
 *    → 从报错里解析出上限并学会预钳制。
 *
 * 学习记在内存（重启后最多白学一次）；触发时先学习、再对同一候选原样重试一次，
 * 重试仍失败回吐原始 400。治的是"参数不兼容 → 整轮换家"的无谓轮换——
 * 错在参数形状不在渠道健康，换下一家只会把同形状的参数再撞一遍。
 */
@Service
public class ParamHealer {

    private static final Logger log = LoggerFactory.getLogger(ParamHealer.class);

    /** 单次转发最多自愈几次（strip 与 cap 是两类独立错误，各自可能撞上一次） */
    public static final int MAX_HEALS_PER_CALL = 2;

    /** provider|model → 该家拒绝的顶层参数名集合（发前预剥离） */
    private final Map<String, List<String>> stripLearned = new ConcurrentHashMap<>();
    /** provider|model → 该家 max_tokens 上限（发前预钳制） */
    private final Map<String, Integer> capLearned = new ConcurrentHashMap<>();

    // ================= 纯函数（可单测）：错误体 → 自愈动作 =================

    /**
     * 错误体是否为"reasoning_effort 组合冲突"→ 应剥离的顶层参数名。
     * 只剥 reasoning_effort（报错点名的是"两个都有"，保留结构化 reasoning 对象）。
     */
    public static List<String> conflictParamsToStrip(String errBody) {
        if (errBody == null || errBody.isBlank()) return List.of();
        String low = errBody.toLowerCase();
        if (!low.contains("reasoning_effort")) return List.of();
        if (low.contains("cannot specify both") || low.contains("use one or the other")
                || low.contains("both reasoning_effort") || low.contains("duplicate")
                || low.contains("unexpected") || low.contains("unknown parameter")) {
            return List.of("reasoning_effort");
        }
        return List.of();
    }

    /** 数字在关键词后的上限话术：中文"不能超过 65536"、英文"maximum of 65536"/"at most 65536" */
    private static final Pattern MAX_TOKENS_CAP_AFTER = Pattern.compile(
            "(?:不能超过|不得超过|maximum(?: of)?|max(?:imum)?\\s*(?:is|of|value)?|up to|at most|less than(?: or equal)? to)\\D{0,6}([0-9]{3,9})",
            Pattern.CASE_INSENSITIVE);

    /** 错误体里的 max_tokens 上限；解析不出返回 -1。兼容中英两种话术；消息里须提到 max_tokens。 */
    public static int parseMaxTokensCap(String errBody) {
        if (errBody == null || errBody.isBlank()) return -1;
        String low = errBody.toLowerCase();
        if (!low.contains("max_tokens") && !low.contains("max tokens")) return -1;
        Matcher m = MAX_TOKENS_CAP_AFTER.matcher(errBody);
        while (m.find()) {
            try {
                int cap = Integer.parseInt(m.group(1));
                if (cap > 0) return cap;
            } catch (NumberFormatException ignore) {
                // 继续找下一处数字
            }
        }
        return -1;
    }

    // ================= 实例状态：学习 + 预适配 =================

    private static String key(String providerId, String model) {
        return providerId + "|" + model;
    }

    /** 学习：该家该模型拒绝这些顶层参数（此后每个请求发前预剥离） */
    public void learnStrip(String providerId, String model, List<String> params) {
        if (params == null || params.isEmpty()) return;
        stripLearned.merge(key(providerId, model), params,
                (oldV, newV) -> { var merged = new java.util.ArrayList<>(oldV); for (String p : newV) if (!merged.contains(p)) merged.add(p); return merged; });
        log.info("[参数自愈] {}/{} 学会预剥离 {}（此后该家请求不再带这些参数）", providerId, model, params);
    }

    /** 学习：该家该模型的 max_tokens 上限（此后预钳制） */
    public void learnMaxTokensCap(String providerId, String model, int cap) {
        if (cap <= 0) return;
        capLearned.merge(key(providerId, model), cap, Math::min);
        log.info("[参数自愈] {}/{} 学会 max_tokens 上限 {}（此后预钳制）", providerId, model, cap);
    }

    /** 一次可执行的自愈动作（null=本次错误无对应自愈项） */
    public record Heal(List<String> strip, int cap) {}

    /** 从错误体识别自愈动作：先看参数组合冲突，再看 max_tokens 上限 */
    public static Heal detectHeal(String errBody) {
        List<String> strip = conflictParamsToStrip(errBody);
        if (!strip.isEmpty()) return new Heal(strip, -1);
        int cap = parseMaxTokensCap(errBody);
        if (cap > 0) return new Heal(List.of(), cap);
        return null;
    }

    /** 学习并把动作应用到请求副本上（防御性副本，绝不改调用方的 map）；返回 null 表示动作无效 */
    public Map<String, Object> learnAndApply(String providerId, String model,
                                             Map<String, Object> openAiRequest, Heal h) {
        if (h == null) return null;
        if (h.strip() != null && !h.strip().isEmpty()) learnStrip(providerId, model, h.strip());
        if (h.cap() > 0) learnMaxTokensCap(providerId, model, h.cap());
        Map<String, Object> out = new java.util.LinkedHashMap<>(openAiRequest);
        if (h.strip() != null) h.strip().forEach(out::remove);
        if (h.cap() > 0) {
            Object mt = out.get("max_tokens");
            int reqMt = mt == null ? 0 : com.gw.provider.adaptor.Json.longVal(mt).intValue();
            if (reqMt == 0 || reqMt > h.cap()) out.put("max_tokens", h.cap());
        }
        return out;
    }

    /**
     * 发前预适配：按已学习口径剥离参数/钳制 max_tokens。
     * 没有学习记录时原样返回同一引用（零拷贝零行为变化）；有适配时返回防御性副本（绝不改调用方的 map）。
     */
    public Map<String, Object> preAdapt(String providerId, String model, Map<String, Object> openAiRequest) {
        List<String> strip = stripLearned.get(key(providerId, model));
        Integer cap = capLearned.get(key(providerId, model));
        if ((strip == null || strip.isEmpty()) && cap == null) return openAiRequest;
        Map<String, Object> out = new java.util.LinkedHashMap<>(openAiRequest);
        if (strip != null) strip.forEach(out::remove);
        if (cap != null) {
            Object mt = out.get("max_tokens");
            int reqMt = mt == null ? 0 : com.gw.provider.adaptor.Json.longVal(mt).intValue();
            if (reqMt == 0 || reqMt > cap) out.put("max_tokens", cap);
        }
        return out;
    }

    /** 本次调用已自愈过几次（防循环：自愈重试再失败就不无限重试） */
    public boolean canHeal(int healsSoFar) {
        return healsSoFar < MAX_HEALS_PER_CALL;
    }
}
