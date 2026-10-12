package com.gw;

import com.gw.policy.AdaptiveRateLimiter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自适应限流学习器（动态版）单元自测——main 运行式（无 JUnit 依赖，与项目其他测试一致）：
 *   run: mvn -q -DskipTests compile test-compile 后
 *        java -cp "target/test-classes;target/classes" com.gw.AdaptiveRateLimiterTest
 * 覆盖 8 个动态行为：
 *   1. 向下收敛：触发 429 收紧上限
 *   2. 向上试探：429 后长时间无 429 且有足量流量，上限自动放宽
 *   3. 时段感知：高峰/低峰分开记录，各自独立
 *   4. 时间淡出：6 小时无流量上限归零（强制重学）
 *   5. 多 Key(账号) 隔离：单 key 耗尽只降权该 key，不连坐同 provider 其他 key
 *   6. 撞墙密度：近 5 分钟撞 429 计次、按渠道隔离、样本滑出即自动复原
 *   7. L 因子映射：瞬时压力与撞墙密度取更狠的一侧，地板 0.2、密度满 0.35
 *   8. 撤席门槛：持续撞墙 + 学到的额度本来就紧才撤席；跨 Key 取最紧的那把
 *   9. 慢性限流：长窗口 429 占比（读库、重启不丢）的降权与撤席门槛
 */
public class AdaptiveRateLimiterTest {

    static final AtomicLong NOW = new AtomicLong(System.currentTimeMillis());

    static AdaptiveRateLimiter newLimiter() {
        AdaptiveRateLimiter l = new AdaptiveRateLimiter(null);
        l.setTime(() -> NOW.get());
        return l;
    }

    static int failures = 0;
    static void check(String name, boolean ok, String detail) {
        System.out.println((ok ? "  ✓ " : "  ✗ FAIL ") + name + (ok ? "" : "  ← " + detail));
        if (!ok) failures++;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("== 1. 向下收敛 ==");
        AdaptiveRateLimiter a = newLimiter();
        NOW.set(morning(10, 0));                      // 高峰时段（10点）
        for (int i = 0; i < 30; i++) {
            a.observe("sensenova", "ds4f", "ch1", 100 + i * 10);
            NOW.addAndGet(1000);                       // 每秒一个
        }
        a.recordRateLimit("sensenova", "ds4f", "ch1", 120);  // 第31个时触发429
        double pressure = a.ratePressure("sensenova", "ds4f", "ch1");
        check("高峰上限被学出(压力>0)", pressure > 0,
                "pressure=" + String.format("%.2f", pressure));

        System.out.println("== 2. 向上试探(市场放款) ==");
        AdaptiveRateLimiter b = newLimiter();
        NOW.set(morning(10, 0));
        for (int i = 0; i < 12; i++) { b.observe("tokenrouter", "glm", "ch1", 50); NOW.addAndGet(1000); }
        b.recordRateLimit("tokenrouter", "glm", "ch1", 50);
        double p1 = b.ratePressure("tokenrouter", "glm", "ch1");
        // 快进 21 分钟(过 20min relax 门槛),再给足量流量(≥8/min)且无 429 → 触发一次 relax(受 5min 冷却限制)
        NOW.addAndGet(21 * 60_000L);
        for (int i = 0; i < 10; i++) { b.observe("tokenrouter", "glm", "ch1", 50); NOW.addAndGet(1000); }
        double p2 = b.ratePressure("tokenrouter", "glm", "ch1");
        check("无429且流量足→上限放宽(压力下降)", p2 < p1,
                String.format("p1=%.2f p2=%.2f", p1, p2));
        // 冷却保护：5 分钟内不连环放宽（再给流量应该不再显著下降或放宽）
        double beforeRelax = b.ratePressure("tokenrouter", "glm", "ch1");
        for (int i = 0; i < 10; i++) { b.observe("tokenrouter", "glm", "ch1", 50); NOW.addAndGet(1000); }
        double afterRelax = b.ratePressure("tokenrouter", "glm", "ch1");
        check("5min冷却→不连环放宽", afterRelax >= beforeRelax - 0.001,
                String.format("before=%.3f after=%.3f", beforeRelax, afterRelax));

        System.out.println("== 3. 时段感知(高峰/低峰分开) ==");
        AdaptiveRateLimiter c = newLimiter();
        NOW.set(morning(10, 0));                           // 高峰
        for (int i = 0; i < 20; i++) { c.observe("amd", "qwen", "ch1", 10); NOW.addAndGet(1000); }
        c.recordRateLimit("amd", "qwen", "ch1", 10);       // 高峰学到上限
        double peakP = c.ratePressure("amd", "qwen", "ch1");
        NOW.set(night(2, 0));                              // 切到低峰(凌晨2点)
        double lowP = c.ratePressure("amd", "qwen", "ch1");
        check("低峰时段压力≠高峰(时段独立)", lowP != peakP, "");
        // 低峰重新学(低峰无上限时压力应为0)
        check("低峰初始无上限→压力0", lowP == 0.0, "lowP=" + lowP);

        System.out.println("== 4. 时间淡出(6小时无流量→重学) ==");
        AdaptiveRateLimiter d = newLimiter();
        NOW.set(morning(10, 0));
        for (int i = 0; i < 15; i++) { d.observe("zhipu", "glm", "ch1", 10); NOW.addAndGet(1000); }
        d.recordRateLimit("zhipu", "glm", "ch1", 10);
        double before = d.ratePressure("zhipu", "glm", "ch1");
        NOW.set(morning(10, 0) + 7 * 3600_000L);           // 快进7小时(超6h stale)
        double afterStale = d.ratePressure("zhipu", "glm", "ch1"); // ceilingFor 触发淡出 → 归零
        check("6h无流量→上限淡出(压力归0)", afterStale == 0.0 && before > 0,
                String.format("before=%.2f after=%.2f", before, afterStale));

        System.out.println("== 5. 多Key(账号)隔离：单key耗尽不连坐其他key ==");
        AdaptiveRateLimiter e = newLimiter();
        NOW.set(morning(10, 0));
        // 同一 provider(amd) 两把 key：ch1 打满触发 429，ch2 无流量
        for (int i = 0; i < 30; i++) { e.observe("amd", "MiniCPM", "ch1", 100); NOW.addAndGet(1000); }
        e.recordRateLimit("amd", "MiniCPM", "ch1", 100);   // ch1 429 收紧
        double pCh1 = e.ratePressure("amd", "MiniCPM", "ch1");
        double pCh2 = e.ratePressure("amd", "MiniCPM", "ch2");
        check("ch1 触发限流后压力>0", pCh1 > 0, "pCh1=" + pCh1);
        check("ch2 不受 ch1 影响(压力=0)", pCh2 == 0.0, "pCh2=" + pCh2);
        // reset 按家清空：两把 key 都归零
        e.reset("amd");
        check("reset按家清空ch1", e.ratePressure("amd", "MiniCPM", "ch1") == 0.0, "");
        // provider 级退化 key（channel=null）应独立于 key 级
        e.observe("amd", "MiniCPM", null, 10);
        e.recordRateLimit("amd", "MiniCPM", null, 10);
        check("provider级退化key与key级隔离", e.ratePressure("amd", "MiniCPM", null) > 0
                && e.ratePressure("amd", "MiniCPM", "ch1") == 0.0, "");

        System.out.println("== 6. 撞墙密度：窗口内计次、按渠道隔离、样本滑出即复原 ==");
        AdaptiveRateLimiter f = newLimiter();
        NOW.set(morning(10, 0));
        for (int i = 0; i < 4; i++) {
            f.recordRateLimit("sensenova", "ds4f", "ch1", 100);
            NOW.addAndGet(10_000);                       // 5 分钟内撞 4 次
        }
        check("4 次撞墙→家级密度 1.0", f.limitDensity("sensenova") >= 0.999,
                "density=" + f.limitDensity("sensenova"));
        check("渠道级密度同值", f.channelLimitDensity("sensenova", "ch1") >= 0.999, "");
        check("另一把 Key 不被连坐（渠道级密度 0）", f.channelLimitDensity("sensenova", "ch2") == 0.0, "");
        check("撞墙次数==4", f.providerRateLimits("sensenova") == 4, "n=" + f.providerRateLimits("sensenova"));
        NOW.addAndGet(6 * 60_000L);                       // 停手 6 分钟：样本全部滑出窗口
        check("样本滑出→密度自动归零（不需人工解除）", f.limitDensity("sensenova") == 0.0,
                "density=" + f.limitDensity("sensenova"));

        System.out.println("== 7. L 因子 = 瞬时压力与撞墙密度取更狠的一侧 ==");
        check("无压力无密度→1.0（没撞过墙的家不受影响）",
                AdaptiveRateLimiter.pressureFactor(0, 0) == 1.0, "");
        check("瞬时压力满→0.2（与改造前一致）",
                AdaptiveRateLimiter.pressureFactor(1.0, 0) == 0.2,
                "f=" + AdaptiveRateLimiter.pressureFactor(1.0, 0));
        check("密度满→0.35（本轮新增的地板）",
                AdaptiveRateLimiter.pressureFactor(0, 1.0) == 0.35,
                "f=" + AdaptiveRateLimiter.pressureFactor(0, 1.0));
        check("两者都满→取更狠的 0.2，不低于地板",
                AdaptiveRateLimiter.pressureFactor(1.0, 1.0) == 0.2, "");
        check("窗口有余量但一直在撞墙→照样被罚（这就是修掉的盲区）",
                AdaptiveRateLimiter.pressureFactor(0, 0.5) < 0.75,
                "f=" + AdaptiveRateLimiter.pressureFactor(0, 0.5));
        check("越界输入被夹住（>1 与 <0 都不炸）",
                AdaptiveRateLimiter.pressureFactor(5, 5) == 0.2
                        && AdaptiveRateLimiter.pressureFactor(-1, -1) == 1.0, "");

        System.out.println("== 8. 撤席门槛：持续撞墙 + 额度本来就紧；跨 Key 取最紧 ==");
        check("3 次/紧额度(2 次每分)→撤席", AdaptiveRateLimiter.seatBlockedByRateLimit(3, 2), "");
        check("2 次→偶发抖动，不撤席", !AdaptiveRateLimiter.seatBlockedByRateLimit(2, 2), "");
        check("额度还没学到(0)→不撤席（无证据不惩罚）", !AdaptiveRateLimiter.seatBlockedByRateLimit(4, 0), "");
        check("额度宽(6 次每分)→撞 4 次也保留席位", !AdaptiveRateLimiter.seatBlockedByRateLimit(4, 6), "");
        check("边界值 5 次每分算紧", AdaptiveRateLimiter.seatBlockedByRateLimit(3, 5), "");
        AdaptiveRateLimiter g = newLimiter();
        NOW.set(morning(10, 0));
        for (int i = 0; i < 12; i++) { g.observe("amd", "m1", "ch1", 10); NOW.addAndGet(1000); }
        g.recordRateLimit("amd", "m1", "ch1", 10);            // ch1 学到 ≈13 次/分
        NOW.addAndGet(60_000L);                                // 让 ch1 的窗口滑走，ch2 单独学
        for (int i = 0; i < 3; i++) { g.observe("amd", "m1", "ch2", 10); NOW.addAndGet(1000); }
        g.recordRateLimit("amd", "m1", "ch2", 10);
        g.recordRateLimit("amd", "m1", "ch2", 10);            // ch2 学到 ≈4 次/分（更紧）
        check("跨 Key 汇总撞墙次数==3", g.providerRateLimits("amd") == 3,
                "n=" + g.providerRateLimits("amd"));
        check("最紧上限取到 ch2（≤5 次每分）", g.tightestReqCeiling("amd") > 0 && g.tightestReqCeiling("amd") <= 5,
                "ceiling=" + g.tightestReqCeiling("amd"));
        check("该家据此应被撤席",
                AdaptiveRateLimiter.seatBlockedByRateLimit(g.providerRateLimits("amd"), g.tightestReqCeiling("amd")), "");
        check("别家不受影响（无留痕→0 次、无上限、不撤席）",
                g.providerRateLimits("zhipu") == 0 && g.tightestReqCeiling("zhipu") == 0, "");
        g.reset("amd");
        check("reset 按家清掉撞墙样本", g.providerRateLimits("amd") == 0, "");

        System.out.println("== 9. 慢性限流（第三只眼：长窗口读库，重启不丢）==");
        // 前面 6/8 号用例暴露的盲区：两次 429 相隔 6 分钟 → 密度归零、L=1.0、抽签席照占，
        // 可这家其实整天都在撞墙（实测某免费上游三天 106 发里 79 发 429）。
        check("纯函数：达撤席线且样本够→撤席",
                AdaptiveRateLimiter.seatBlockedByChronic(0.35, 20, 0.3, 8), "");
        check("纯函数：比例够但样本太少不判（防 3 发里 2 发 429 误伤）",
                !AdaptiveRateLimiter.seatBlockedByChronic(0.9, 5, 0.3, 8), "");
        check("纯函数：没撞过墙（比例 0）→ 绝不撤席",
                !AdaptiveRateLimiter.seatBlockedByChronic(0.0, 500, 0.3, 8), "");
        check("纯函数：撤席线=0 表示关掉这一层",
                !AdaptiveRateLimiter.seatBlockedByChronic(0.9, 500, 0.0, 8), "");
        check("降权：达线给地板 0.2",
                AdaptiveRateLimiter.chronicPenaltyFactor(0.6, 50, 8, 0.3) == 0.2,
                "f=" + AdaptiveRateLimiter.chronicPenaltyFactor(0.6, 50, 8, 0.3));
        check("降权：线的一半→线性给 0.6（不是开关式跳变）",
                Math.abs(AdaptiveRateLimiter.chronicPenaltyFactor(0.15, 50, 8, 0.3) - 0.6) < 1e-9,
                "f=" + AdaptiveRateLimiter.chronicPenaltyFactor(0.15, 50, 8, 0.3));
        check("降权：样本不足→1.0（未知不罚）",
                AdaptiveRateLimiter.chronicPenaltyFactor(0.9, 7, 8, 0.3) == 1.0, "");
        AdaptiveRateLimiter h = newLimiter();
        NOW.set(morning(10, 0));
        Map<String, long[]> stats = new java.util.HashMap<>();
        stats.put("sensenova", new long[]{100, 40});     // 100 发里 40 发 429 = 40%
        stats.put("agnes", new long[]{100, 2});          // 偶发抖动
        h.setChronicStats(stats, NOW.get());
        check("读库后的占比算得对（40%）", Math.abs(h.chronicLimitRatio("sensenova") - 0.4) < 1e-9,
                "ratio=" + h.chronicLimitRatio("sensenova"));
        check("没见过数据的家占比 0", h.chronicLimitRatio("unknown") == 0.0, "");
        check("样本数可查（界面要写明凭什么撤席）", h.chronicSamples("sensenova") == 100, "");
        check("达线家：撤席 + 压到地板",
                AdaptiveRateLimiter.seatBlockedByChronic(h.chronicLimitRatio("sensenova"),
                        h.chronicSamples("sensenova"), 0.3, 8)
                        && h.chronicPenalty("sensenova", 8, 0.3) == 0.2, "");
        check("抖动家（2%）：保留席位、几乎不罚",
                !AdaptiveRateLimiter.seatBlockedByChronic(h.chronicLimitRatio("agnes"),
                        h.chronicSamples("agnes"), 0.3, 8)
                        && h.chronicPenalty("agnes", 8, 0.3) > 0.9,
                "f=" + h.chronicPenalty("agnes", 8, 0.3));
        // 管理页"重新学习"必须连慢性层一起清，否则 60 秒后又靠同一份库数据把席撤了
        h.reset("sensenova");
        check("reset 一起清慢性层", h.chronicLimitRatio("sensenova") == 0.0
                && h.chronicSamples("sensenova") == 0 && h.chronicPenalty("sensenova", 8, 0.3) == 1.0, "");
        check("reset 不连坐别家", h.chronicLimitRatio("agnes") > 0.0, "");
        h.setChronicStats(Map.of(), 0);
        check("传空=关掉这一层（全部占比归零、不罚）",
                h.chronicLimitRatio("agnes") == 0.0 && h.chronicPenalty("agnes", 8, 0.3) == 1.0, "");

        System.out.println();
        System.out.println(failures == 0 ? "全部通过" : (failures + " 个失败"));
        if (failures > 0) System.exit(1);
    }

    /** 生成某日某时刻的 epoch（测试可控）*/
    static long morning(int h, int min) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.set(java.util.Calendar.HOUR_OF_DAY, h);
        cal.set(java.util.Calendar.MINUTE, min);
        cal.set(java.util.Calendar.SECOND, 0);
        cal.set(java.util.Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }
    static long night(int h, int min) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.set(java.util.Calendar.HOUR_OF_DAY, h);
        cal.set(java.util.Calendar.MINUTE, min);
        cal.set(java.util.Calendar.SECOND, 0);
        cal.set(java.util.Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }
}
