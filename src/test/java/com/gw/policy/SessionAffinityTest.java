package com.gw.policy;

import java.util.ArrayList;
import java.util.List;

/**
 * 快速单测（main 运行）：会话粘性路由（2026-11-18 批）。
 * 验证：钉住→提到队首；连败 2 次解钉；非钉住者失败不解钉；成功重钉；键稳定与可分。
 * SessionAffinity 用 null ConfigService 构造（enabled() 异常兜底按开启）。
 */
public class SessionAffinityTest {

    static int fails = 0;

    static void check(boolean ok, String name, String detail) {
        System.out.printf("%s %s%s%n", ok ? "PASS" : "FAIL", name, detail == null ? "" : "  ← " + detail);
        if (!ok) fails++;
    }

    static PolicyService.Scored s(String pid, String model, String ch) {
        return new PolicyService.Scored(new PolicyService.Candidate(pid, model, ch, 1, null), 1.0, false);
    }

    static List<PolicyService.Scored> pool() {
        List<PolicyService.Scored> l = new ArrayList<>();
        l.add(s("amd", "DeepSeek-V4-Flash", "main"));
        l.add(s("stepfun", "stepaudio-3-chat-preview", "main"));
        l.add(s("deepseek", "deepseek-v4-pro", "main"));
        return l;
    }

    public static void main(String[] args) {
        SessionAffinity sa = new SessionAffinity(null);

        // ---- 键稳定：同消息同键；换 client/换模型换键 ----
        java.util.Map<String, Object> req = new java.util.LinkedHashMap<>();
        req.put("messages", List.of(
                java.util.Map.of("role", "system", "content", "系统提示词ABC"),
                java.util.Map.of("role", "user", "content", "第一条指令"),
                java.util.Map.of("role", "assistant", "content", "好"),
                java.util.Map.of("role", "user", "content", "第二条指令")));
        String k1 = sa.keyOf(req, "zcode", "free-fast");
        String k2 = sa.keyOf(req, "zcode", "free-fast");
        String k3 = sa.keyOf(req, "claude-code", "free-fast");
        String k4 = sa.keyOf(req, "zcode", "glm");
        check(k1.equals(k2), "同请求同键", null);
        check(!k1.equals(k3), "换 client 换键", null);
        check(!k1.equals(k4), "换模型换键", null);

        // ---- 成功后钉住：apply 把钉住者从第 3 位提到队首，其余保序 ----
        sa.recordSuccess(k1, new PolicyService.Candidate("deepseek", "deepseek-v4-pro", "main", 1, null));
        List<PolicyService.Scored> out = sa.apply(k1, pool());
        check("deepseek".equals(out.get(0).candidate.providerId())
                && "deepseek-v4-pro".equals(out.get(0).candidate.model()),
                "钉住者提到队首", out.get(0).candidate.providerId() + "/" + out.get(0).candidate.model());
        check("amd".equals(out.get(1).candidate.providerId()) && "stepfun".equals(out.get(2).candidate.providerId()),
                "其余候选保序", null);

        // ---- 未钉会话 apply 原样 ----
        List<PolicyService.Scored> untouched = sa.apply("nobody", pool());
        check("amd".equals(untouched.get(0).candidate.providerId()), "无钉原样返回", null);

        // ---- 连败 2 次解钉：第 1 次仍钉（给重试机会），第 2 次不再钉 ----
        PolicyService.Candidate pinned = new PolicyService.Candidate("deepseek", "deepseek-v4-pro", "main", 1, null);
        sa.recordFailure(k1, pinned);
        List<PolicyService.Scored> stillPinned = sa.apply(k1, pool());
        check("deepseek".equals(stillPinned.get(0).candidate.providerId()), "连败 1 次仍钉", null);
        sa.recordFailure(k1, pinned);
        List<PolicyService.Scored> unpinned = sa.apply(k1, pool());
        check("amd".equals(unpinned.get(0).candidate.providerId()), "连败 2 次解钉", null);

        // ---- 非钉住者失败不影响钉 ----
        sa.recordSuccess(k1, pinned);
        sa.recordFailure(k1, new PolicyService.Candidate("amd", "DeepSeek-V4-Flash", "main", 1, null));
        List<PolicyService.Scored> still = sa.apply(k1, pool());
        check("deepseek".equals(still.get(0).candidate.providerId()), "别人失败不解钉", null);

        // ---- 失败的钉住者不在 rank 名单（冷却）→ apply 原样（天然解钉不误钉） ----
        sa.recordSuccess(k1, pinned);
        sa.recordFailure(k1, pinned);          // 1 次连败
        List<PolicyService.Scored> without = new ArrayList<>(pool());
        without.removeIf(x -> x.candidate.providerId().equals("deepseek"));
        List<PolicyService.Scored> outNo = sa.apply(k1, without);
        check("amd".equals(outNo.get(0).candidate.providerId()), "钉住者不在名单→不钉", null);

        System.out.println(fails == 0 ? "\nALL PASS" : "\nFAILED: " + fails);
        if (fails > 0) System.exit(1);
    }
}
