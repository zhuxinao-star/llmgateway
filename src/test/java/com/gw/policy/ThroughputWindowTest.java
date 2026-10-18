package com.gw.policy;

/**
 * 吞吐样本的出字窗口口径（main 直跑）。
 *
 * 2026-10-16 从现网库里数出来的形状：15105 行里 307 行 status=OK 却 total_ms &lt; ttft_ms
 *（最狠的一条 ttft 11771 / total 1）。根因不是脏数据，是两把钟——流式落库的 total 从
 * streamToClient 起算，而 ttft 取 max(execute 里量到的上游首字, relay 内首字偏移)，
 * 前者把"上游等待"整段漏掉了。既然两个数不同源，"总时长比首字还短"就是必然出现而不是偶发。
 *
 * 这个口径直接影响分摊份额：吞吐因子 T = tokens/分母，分母一旦被写成"只算 relay 那几毫秒"，
 * 一次 2 token 的短答就算出 400 tokens/秒，把一家慢模型洗成快模型抢走份额。
 * 修完之后 total 从接单起算（total ≥ ttft 成为不变量），T 改用"首字之后的出字窗口"，
 * 缓冲式响应（没有逐字窗口）才退回整窗。
 *
 * 钉四件事：
 *  ① 分母永远落在 [0, totalMs]：负数绝不出现（旧脏行 total&lt;ttft 传进来也只退回整窗，不把速率算反），
 *    0 只在压根没有时长时出现，而 0 分母＝这条不记吞吐样本；
 *  ② 流式用"首字之后"那段——同一份 tokens，预填充慢不该让出字速度看起来也慢（S 已经罚过一次）；
 *  ③ 非流式/缓冲式（gen 短于 MIN_GENERATION_WINDOW_MS）退回整窗，绝不拿 3ms 除几百 token；
 *  ④ totalMs ≤ 0 或 tokensOut 缺失时不给样本（宁可没有，也不要一个编出来的速率）。
 */
public class ThroughputWindowTest {

    static int failures = 0;

    static void check(String name, boolean cond) {
        check(name, cond, "");
    }

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    /** 与 PolicyService 内部常量对齐：短于这段就认定"没有可观测的逐字窗口" */
    static final int MIN_GEN = 250;

    /** 只测"有首字数字"的那些组合：用 long 免掉每处都写 L，null 那条单独直接调 */
    static long w(long totalMs, long ttftMs) {
        return PolicyService.throughputWindowMs(totalMs, ttftMs);
    }

    public static void main(String[] args) {
        // ① 流式：真实出字窗口＝total - ttft
        long total = 9_059, ttft = 4_638;   // claude-opus-5-5 现网一条真行的形状（首字 4.6s、又跑了 4.4s 出完）
        long win = w(total, ttft);
        check("流式按首字之后的窗口算", win == total - ttft, "got=" + win);
        check("流式分母不含预填充（同一行整窗要慢一倍）", win < total, "win=" + win + " total=" + total);

        // ② 非流式/缓冲：ttft 就是整个上游等待，差值≈0 → 退回整窗
        check("缓冲式退回整窗", w(4_500, 4_498) == 4_500,
                "got=" + w(4_500, 4_498));
        check("差值刚好到线才认逐字窗", w(4_000, 4_000 - MIN_GEN) == MIN_GEN,
                "got=" + w(4_000, 4_000 - MIN_GEN));
        check("差值差一条仍退回整窗", w(4_000, 4_000 - (MIN_GEN - 1)) == 4_000,
                "got=" + w(4_000, 4_000 - (MIN_GEN - 1)));

        // ③ 无首字样本（压缩子调用、非流式老调用方传 null）：整窗
        check("无首字样本用整窗", PolicyService.throughputWindowMs(1_200, null) == 1_200);
        check("首字为负(无样本哨兵)用整窗", w(1_200, -1L) == 1_200);

        // ④ 现网那批两把钟脏行：total < ttft 也绝不返回负数/零
        check("旧脏行不把速率算反", w(15, 11_771) == 15,
                "got=" + w(15, 11_771));
        check("脏行不产生 0 分母", w(1, 639) > 0);

        // ⑤ 没有时长就没有样本（不许编）
        check("零时长不给分母", w(0, 500) == 0);
        check("负时长不给分母", w(-3, 500) == 0);

        // ⑥ 修口径前后同一条流量的对比：短答被 relay 窗洗成"飞快"
        long tokens = 2;
        double beforeTps = tokens * 1000.0 / 5;               // 旧：只算 relay 那 5ms
        double afterTps = tokens * 1000.0 / w(11_776, 11_771);
        check("修口径后同一条短答不再虚高", afterTps < beforeTps / 10,
                "before=" + beforeTps + " after=" + afterTps);

        // ⑦ 不变量：任何输入都落在 [0, total]
        boolean bounded = true;
        long[] totals = {0, 1, 5, 300, 4_000, 9_059, 120_000};
        Long[] ts = {null, -1L, 0L, 1L, 4_498L, 4_638L, 9_059L, 120_000L, 300_000L};
        for (long t : totals) {
            for (Long x : ts) {
                long got = PolicyService.throughputWindowMs(t, x);   // 直接调：x 可能是 null，不能过 long 形参
                if (got > t || got < 0) {
                    bounded = false;
                    check("分母越界 total=" + t + " ttft=" + x, false, "got=" + got);
                }
            }
        }
        check("分母恒在 [0, totalMs] 内", bounded);

        System.out.println(failures == 0 ? "[ThroughputWindowTest] all checks passed"
                : "[ThroughputWindowTest] FAILURES=" + failures);
        if (failures > 0) System.exit(1);
    }
}
