package com.gw.store;

import com.gw.policy.AdaptiveRateLimiter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * 慢性限流取数单测（main 直跑）：request_log 里"从未选路"的脏行绝不能变成某家供应商的限流证据
 * （2026-10-14 实锤）。一条 provider_id 为空的脏行 → 结果 Map 带 null key → Map.copyOf 抛 NPE
 * → 60 秒快照调度整条打死，慢性层与运行态快照同时冻结 28 分钟，期间 sensenova 11 发 9 中 429
 * （占比 0.82，早已过 0.3 撤席线）却没被隔离，流量全 fallback 到一家，分摊彻底失真。
 * 三层防线各自断言：
 *   ① SQL 口径：NULL / '' / '-' 三家不存在的"家"都不进结果，真实家按 (总数, 429 数) 记对；
 *   ② 巡检流量（probe/selfcheck/reconcile）不算真实样本，窗口外的行不算；
 *   ③ setChronicStats 兜脏 key：任何上游取数口径再出问题也只是丢条目，绝不能炸掉调度。
 * 用临时 SQLite，不碰真实 data/ 库。
 */
public class ChronicRateLimitQueryTest {

    static int failures = 0;

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    static void checkEq(String name, Object expected, Object actual) {
        boolean ok = expected == null ? actual == null : expected.equals(actual);
        System.out.println((ok ? "PASS " : "FAIL ") + name + (ok ? "" : "  期望=" + expected + " 实际=" + actual));
        if (!ok) failures++;
    }

    /** 18 参落库：provider 可为 null/''/'-'，client 可为 probe 等巡检值 */
    static void row(StatsDao dao, long ts, String provider, String errorClass, String client) {
        boolean failed = errorClass != null;
        dao.insertRequestLog(ts, "m1", provider, "m1",
                failed ? null : 300, 1000, failed ? null : 5L, failed ? null : 2L,
                failed ? "FAILED" : "OK", 1, null, failed ? "429 Too Many Requests" : null,
                client, null, null, failed ? 429 : 200, errorClass, null);
    }

    static long countAll(Database db) throws java.sql.SQLException {
        Long v = db.queryOne("SELECT COUNT(*) FROM request_log", rs -> rs.getLong(1));
        return v == null ? -1 : v;
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("gw-chronic");
        long now = System.currentTimeMillis();
        long winMs = 60 * 60_000L;                       // 与线上 chronicRateWindowMinutes=60 同口径
        long since = now - winMs;

        try (Database db = new Database(dir.resolve("t.db").toString(),
                dir.resolve("config.yaml").toString())) {
            StatsDao dao = new StatsDao(db);

            // sensenova：9 发 429 + 2 发成功 = 真实 11 样 9 限流（今天的实测形状，该撤席）
            for (int i = 0; i < 9; i++) row(dao, now - 1000 - i, "sensenova", "RATE_LIMIT", "claude-code");
            for (int i = 0; i < 2; i++) row(dao, now - 2000 - i, "sensenova", null, "claude-code");
            // lingxiongkeji：6 发全成功，其中 1 发是 AUTH 类错误（不计入 429 数）
            for (int i = 0; i < 5; i++) row(dao, now - 3000 - i, "lingxiongkeji", null, "claude-code");
            row(dao, now - 3100, "lingxiongkeji", "AUTH", "curl");

            // 脏行三连：NULL / 空串 / '-' —— 都是"网关侧自拒、从未选路"，不是任何一家的证据
            row(dao, now - 4000, null, "RATE_LIMIT", "claude-code");
            row(dao, now - 4001, "", "RATE_LIMIT", "claude-code");
            row(dao, now - 4002, "-", "RATE_LIMIT", "curl");

            // 巡检流量：三条都不算真实样本（否则探测会把好家冤枉成慢性撞墙）
            row(dao, now - 5000, "sensenova", "RATE_LIMIT", "probe");
            row(dao, now - 5001, "sensenova", "RATE_LIMIT", "selfcheck");
            row(dao, now - 5002, "sensenova", "RATE_LIMIT", "reconcile");

            // 窗口外：7 分钟前的一发不计
            row(dao, since - 60_000, "sensenova", "RATE_LIMIT", "claude-code");

            // 落库走异步写队列：不等排空就查，读到的是空表（会把"过滤生效"误判成"什么都没查出来"）
            int expected = 9 + 2 + 5 + 1 + 3 + 3 + 1;
            long seen = 0;
            for (int i = 0; i < 200 && (seen = countAll(db)) != expected; i++) Thread.sleep(50);
            check("造数全部落库（异步写队列已排空）", seen == expected,
                    "seen=" + seen + " expected=" + expected);

            Map<String, long[]> stats = dao.chronicRateLimits(since);

            // ---- ① 结果里绝不许出现 null/空/'-' 这类 key ----
            check("① 无 null key（今天炸调度的就是它）", !stats.containsKey(null),
                    String.valueOf(stats.keySet()));
            check("① 无空串 key", !stats.containsKey("") && !stats.containsKey("  "),
                    String.valueOf(stats.keySet()));
            check("① 无 '-' key", !stats.containsKey("-"), String.valueOf(stats.keySet()));
            for (String k : stats.keySet()) {
                check("① key 非空白：" + k, k != null && !k.isBlank(), String.valueOf(k));
            }
            checkEq("① 只有两家真实供应商", 2, stats.size());

            // ---- ② 计数口径：真实样本数 / 其中 429 数 ----
            long[] sn = stats.get("sensenova");
            check("② sensenova 有数据", sn != null, "null");
            checkEq("② sensenova 真实样本=11（巡检与窗口外不计）", 11L, sn == null ? -1 : sn[0]);
            checkEq("② sensenova 429 数=9", 9L, sn == null ? -1 : sn[1]);
            long[] lx = stats.get("lingxiongkeji");
            checkEq("② lingxiongkeji 样本=6", 6L, lx == null ? -1 : lx[0]);
            checkEq("② lingxiongkeji 429 数=0（AUTH 不算限流）", 0L, lx == null ? -1 : lx[1]);

            // ---- ③ 取数直接接上限流器：不抛异常，且今天该撤席的家真的被撤席 ----
            AdaptiveRateLimiter limiter = new AdaptiveRateLimiter(null);
            boolean threw = false;
            try {
                limiter.setChronicStats(stats, now);
            } catch (Exception e) {
                threw = true;
                check("③ 喂真实取数不抛异常", false, e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            if (!threw) check("③ 喂真实取数不抛异常", true, "");
            double ratio = limiter.chronicLimitRatio("sensenova");
            check("③ 占比算得对（9/11≈0.818）", Math.abs(ratio - 9.0 / 11.0) < 1e-9,
                    "ratio=" + ratio);
            check("③ 过撤席线：0.818 ≥ 0.3 且样本 ≥ 8 → 该家不占抽签席",
                    AdaptiveRateLimiter.seatBlockedByChronic(ratio, limiter.chronicSamples("sensenova"), 0.3, 8),
                    "ratio=" + ratio + " samples=" + limiter.chronicSamples("sensenova"));
            check("③ 干净家（0/6）保留席位且不罚",
                    !AdaptiveRateLimiter.seatBlockedByChronic(limiter.chronicLimitRatio("lingxiongkeji"),
                            limiter.chronicSamples("lingxiongkeji"), 0.3, 8)
                            && limiter.chronicPenalty("lingxiongkeji", 8, 0.3) == 1.0,
                    "f=" + limiter.chronicPenalty("lingxiongkeji", 8, 0.3));

            // ---- ④ 兜底防线：就算别的取数口径塞进脏 key，也只能丢条目不能炸 ----
            Map<String, long[]> dirty = new HashMap<>();
            dirty.put(null, new long[]{9, 9});
            dirty.put("", new long[]{7, 7});
            dirty.put("  ", new long[]{5, 5});
            dirty.put("ok", new long[]{10, 3});
            dirty.put("nullval", null);
            boolean threw2 = false;
            try {
                limiter.setChronicStats(dirty, now);
            } catch (Exception e) {
                threw2 = true;
                check("④ 脏 key 集合不抛异常", false, e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            if (!threw2) check("④ 脏 key 集合不抛异常", true, "");
            checkEq("④ 干净条目原样保留", 10L, limiter.chronicSamples("ok"));
            check("④ 空 key 未进入", limiter.chronicLimitRatio("") == 0.0
                    && limiter.chronicLimitRatio("  ") == 0.0, "");
            check("④ value 为 null 未进入", limiter.chronicSamples("nullval") == 0, "");

            // ---- ⑤ 全脏/传 null = 关掉这一层，不许影响抽签 ----
            limiter.setChronicStats(null, 0);
            check("⑤ 传 null 等于关层（占比 0、降权 1.0、不撤席）",
                    limiter.chronicLimitRatio("sensenova") == 0.0
                            && limiter.chronicPenalty("sensenova", 8, 0.3) == 1.0, "");
        }

        System.out.println();
        if (failures == 0) {
            System.out.println("=== ALL PASS ===");
        } else {
            System.out.println("=== " + failures + " FAILURES ===");
            System.exit(1);
        }
    }
}
