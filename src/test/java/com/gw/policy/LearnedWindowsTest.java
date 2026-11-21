package com.gw.policy;

/**
 * 快速单测（main 运行）：学到的上下文窗口（2026-11-19 批）。
 * 上游以"输入超长"拒绝 N token → 学得窗口 < N；只向下收敛（min 合并）；清理接口齐。
 */
public class LearnedWindowsTest {

    static int fails = 0;

    static void check(boolean ok, String name, String detail) {
        System.out.printf("%s %s%s%n", ok ? "PASS" : "FAIL", name, detail == null ? "" : "  ← " + detail);
        if (!ok) fails++;
    }

    public static void main(String[] args) {
        PolicyService.LearnedWindows w = new PolicyService.LearnedWindows();

        // 没学过 → -1（视为不限，交给配置或上游）
        check(w.ceiling("zhipu", "glm-4.5-flash") == -1, "没学过返回 -1", null);

        // 学一次：拒绝 100000 → 上限 <100000
        w.learn("zhipu", "glm-4.5-flash", 100000);
        check(w.ceiling("zhipu", "glm-4.5-flash") == 99999, "学得窗口=拒绝值-1",
                String.valueOf(w.ceiling("zhipu", "glm-4.5-flash")));

        // 再学更小的 → min 收敛（一次比一次准）
        w.learn("zhipu", "glm-4.5-flash", 60000);
        check(w.ceiling("zhipu", "glm-4.5-flash") == 59999, "更小的拒绝收敛窗口",
                String.valueOf(w.ceiling("zhipu", "glm-4.5-flash")));

        // 再学更大的 → 不放大（min 语义）
        w.learn("zhipu", "glm-4.5-flash", 200000);
        check(w.ceiling("zhipu", "glm-4.5-flash") == 59999, "更大的拒绝不放大窗口", null);

        // 非法输入不学
        w.learn("zhipu", "glm-4.5-flash", 0);
        w.learn("zhipu", "glm-4.5-flash", 1);
        w.learn(null, "glm-4.5-flash", 5000);
        check(w.ceiling("zhipu", "glm-4.5-flash") == 59999, "非法拒绝值不影响已学窗口", null);

        // 按模型清理 / 按供应商清理 / 键隔离
        w.learn("amd", "DeepSeek-V4-Flash", 8000);
        w.learn("amd", "GLM-5.3-Flash", 9000);
        w.clear("amd", "DeepSeek-V4-Flash");
        check(w.ceiling("amd", "DeepSeek-V4-Flash") == -1, "单模型清理生效", null);
        check(w.ceiling("amd", "GLM-5.3-Flash") == 8999, "清理不误伤同家其它模型", null);
        w.clearProvider("amd");
        check(w.ceiling("amd", "GLM-5.3-Flash") == -1, "整供应商清理生效", null);
        check(w.ceiling("zhipu", "glm-4.5-flash") == 59999, "供应商清理不串台", null);
        check(w.size() == 1, "size 只剩 zhipu 一条", String.valueOf(w.size()));

        System.out.println(fails == 0 ? "\nALL PASS" : "\nFAILED: " + fails);
        if (fails > 0) System.exit(1);
    }
}
