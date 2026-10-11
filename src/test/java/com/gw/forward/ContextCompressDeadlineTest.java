package com.gw.forward;

import java.lang.reflect.Method;

/**
 * 快速单测（main 运行）：验证压缩"交互式硬死线"纯决策 nextCompressBudgetMs。
 *
 * 根因回归：非流式压缩子调用过去单条预算恒 120s、整轮候选链无封顶，免费上游(agnes)一挂就吃满
 * 113s 才吐 22 token 垃圾 → 同步阻塞用户那一轮 → Claude Code 客户端断开（"问题没答完就结束"）。
 * 修复：单条 ≤ compressCallTimeoutMs、整轮墙钟 ≤ compressOverallBudgetMs，预算耗尽即停 failover，
 * 走既有 hardTruncateFallback 快速降级。本测试锁定这条决策函数的边界。
 */
public class ContextCompressDeadlineTest {

    public static void main(String[] args) throws Exception {
        Method m = ContextCompressor.class.getDeclaredMethod(
                "nextCompressBudgetMs", long.class, long.class, long.class, int.class);
        m.setAccessible(true);

        boolean ok = true;
        final int FLOOR = 5_000;
        final long CAP = 45_000;      // 单条上限（>健康 sensenova 全量 39s，远低于挂死的 113s）
        final long OVERALL = 60_000;  // 整轮墙钟

        // 用例1：整轮余量充足、单条上限先起效 → 返回 CAP（绝不再给旧值 120s，这是回归锁）
        long now = 1_000_000L;
        long deadline = now + OVERALL;
        int b1 = (int) m.invoke(null, deadline, CAP, now, FLOOR);
        boolean c1 = b1 == CAP;
        System.out.printf("用例1 余量60s/单条上限45s → %dms（期望45000，锁死旧120s）: %s%n", b1, c1 ? "PASS" : "FAIL");
        ok &= c1;

        // 用例2：整轮只剩 8s → 本条只能用 8s（remaining 先起效，绝不超墙钟）
        int b2 = (int) m.invoke(null, deadline, CAP, now + (OVERALL - 8_000), FLOOR);
        boolean c2 = b2 == 8_000;
        System.out.printf("用例2 整轮仅剩8s → %dms（期望8000，不越过墙钟）: %s%n", b2, c2 ? "PASS" : "FAIL");
        ok &= c2;

        // 用例3：整轮剩余低于 floor（只剩3s，不够再发一次有意义的请求）→ 0 = 停止 failover、降级
        int b3 = (int) m.invoke(null, deadline, CAP, now + (OVERALL - 3_000), FLOOR);
        boolean c3 = b3 == 0;
        System.out.printf("用例3 整轮仅剩3s(<floor5s) → %d（期望0=降级，不死等）: %s%n", b3, c3 ? "PASS" : "FAIL");
        ok &= c3;

        // 用例4：墙钟已到点（剩余为负）→ 0，绝不返回负数当超时
        int b4 = (int) m.invoke(null, deadline, CAP, deadline + 10_000, FLOOR);
        boolean c4 = b4 == 0;
        System.out.printf("用例4 已超墙钟 → %d（期望0，不出负值）: %s%n", b4, c4 ? "PASS" : "FAIL");
        ok &= c4;

        // 用例5：模拟真实事件链——首个候选 agnes 若挂满单条上限45s后，整轮仅剩15s(<下一个候选真实所需)，
        // 第二候选请求前算出的预算应 <=15s；若已耗尽则 0。核心断言：单条绝不 >45s，故 agnes 113s 被腰斩。
        int afterAgnesModule = (int) m.invoke(null, deadline, CAP, now + CAP, FLOOR);
        boolean c5 = afterAgnesModule <= (OVERALL - CAP) && afterAgnesModule < 120_000;
        System.out.printf("用例5 agnes 吃掉45s后再决策 → %dms（期望≤15s 且远<120s）: %s%n", afterAgnesModule, c5 ? "PASS" : "FAIL");
        ok &= c5;

        System.out.println("\n=== " + (ok ? "ALL PASS ✅" : "HAS FAILURE ❌") + " ===");
        if (!ok) System.exit(1);
    }
}
