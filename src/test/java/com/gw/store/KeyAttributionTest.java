package com.gw.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Key（渠道）归属单测（main 直跑）：一家供应商配多把 Key 时，"每把分别扛了多少"必须能被回答，
 * 且三条语义红线不能被下游读错：
 *  ① 分组 = provider × model_real × channel_id，每把 Key 各自成行，绝不合并成"这家一共多少"；
 *  ② 上线前的历史行（channel_id 为 NULL，含空串归一后的 NULL）单独成"无归属"桶，
 *     绝不摊给任何一把在用的 Key——否则某把 Key 会被读成凭空多背了一截量；
 *  ③ 网关侧拒绝（provider_id='-'，从未选路）不进这张表，它压根没有 Key。
 * 另外验证窗口/供应商/模型/来源工具过滤真的生效（条件不许被悄悄丢掉）、429 按 error_class 计数、
 * TTFT 中位数只取成功行且偶数取中间两值均值。
 * 用临时 SQLite，不碰真实 data/ 库。
 */
public class KeyAttributionTest {

    static int failures = 0;

    static void check(String name, boolean cond) {
        System.out.println((cond ? "PASS " : "FAIL ") + name);
        if (!cond) failures++;
    }

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    static void checkEq(String name, Object expected, Object actual) {
        // 数值按 longValue 比：SQL 聚合出来一律是 Long，写断言时用 int 字面量不该变成假失败
        boolean ok = (expected instanceof Number e && actual instanceof Number a)
                ? e.longValue() == a.longValue()
                : expected == null ? actual == null : expected.equals(actual);
        System.out.println((ok ? "PASS " : "FAIL ") + name + (ok ? "" : "  期望=" + expected + " 实际=" + actual));
        if (!ok) failures++;
    }

    static long num(Map<String, Object> r, String k) {
        Object v = r.get(k);
        return v instanceof Number n ? n.longValue() : -1;
    }

    static Map<String, Object> row(List<Map<String, Object>> rows, String provider, String model, String channel) {
        for (Map<String, Object> r : rows) {
            if (provider.equals(r.get("provider")) && model.equals(r.get("model"))
                    && channel.equals(String.valueOf(r.get("channel")))) return r;
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("gw-keyattr");
        java.time.LocalDate today = java.time.LocalDate.now();
        long fromMs = today.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        long toMs = today.plusDays(1).atStartOfDay(java.time.ZoneId.systemDefault())
                .toInstant().toEpochMilli() - 1;
        long t0 = today.atTime(12, 0).atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();

        try (Database db = new Database(dir.resolve("t.db").toString(),
                dir.resolve("config.yaml").toString())) {
            StatsDao dao = new StatsDao(db);
            int inserted = 0;

            // alpha/m1 · k1：3 发成功（TTFT 100/200/300）+ 1 发掐断（无 token、TTFT 有值但不算成功样本）
            for (int i = 0; i < 3; i++) {
                dao.insertRequestLog(t0 + i, "m1", "alpha", "m1", 100 + i * 100, 500, 10L, 5L,
                        "OK", 1, null, null, "claude-code", null, null, 200, null, "k1");
                inserted++;
            }
            dao.insertRequestLog(t0 + 22, "m1", "alpha", "m1", 200, 30_000, null, null,
                    "STREAM_TIMEOUT", 1, null, "stream timeout", "claude-code", null, null,
                    null, "UPSTREAM_TIMEOUT", "k1");
            inserted++;
            // alpha/m1 · k2：1 发成功（TTFT 1000）+ 1 发撞 429（status=FAILED、error_class=RATE_LIMIT、无 TTFT）
            dao.insertRequestLog(t0 + 10, "m1", "alpha", "m1", 1000, 1200, 7L, 3L,
                    "OK", 1, null, null, "claude-code", null, null, 200, null, "k2");
            inserted++;
            dao.insertRequestLog(t0 + 11, "m1", "alpha", "m1", null, 30, null, null,
                    "FAILED", 1, null, "429 Too Many Requests", "claude-code", null, null,
                    429, "RATE_LIMIT", "k2");
            inserted++;
            // alpha/m1 · 无归属：2 条 NULL + 1 条空串（空串必须归一成 NULL，与 NULL 同桶）
            dao.insertRequestLog(t0 - 5, "m1", "alpha", "m1", 900, 1000, 1L, 1L,
                    "OK", 1, null, null, "claude-code", null, null, 200, null, null);
            inserted++;
            dao.insertRequestLog(t0 - 4, "m1", "alpha", "m1", 900, 1000, 1L, 1L,
                    "OK", 1, null, null, "claude-code", null, null, 200, null, null);
            inserted++;
            dao.insertRequestLog(t0 - 3, "m1", "alpha", "m1", 900, 1000, 1L, 1L,
                    "OK", 1, null, null, "gw-compress", null, null, 200, null, "");
            inserted++;
            // alpha/m2 · k1（gw-compress）：让"模型过滤"和"来源工具过滤"都有东西可切
            dao.insertRequestLog(t0 + 20, "m2", "alpha", "m2", 60, 90, 2L, 2L,
                    "OK", 1, null, null, "gw-compress", null, null, 200, null, "k1");
            inserted++;
            // beta/m1 · k1：让"供应商过滤"有东西可切
            dao.insertRequestLog(t0 + 21, "m1", "beta", "m1", 50, 80, 1L, 1L,
                    "OK", 1, null, null, "claude-code", null, null, 200, null, "k1");
            inserted++;
            // alpha/m4 · k1：2 个成功样本（100/300）→ 偶数中位数取中间两值均值 = 200
            dao.insertRequestLog(t0 + 24, "m4", "alpha", "m4", 100, 150, 1L, 1L,
                    "OK", 1, null, null, "claude-code", null, null, 200, null, "k1");
            inserted++;
            dao.insertRequestLog(t0 + 25, "m4", "alpha", "m4", 300, 400, 1L, 1L,
                    "OK", 1, null, null, "claude-code", null, null, 200, null, "k1");
            inserted++;
            // 网关侧拒绝：从未选路，既没有供应商也没有 Key → 不进这张表
            dao.insertRequestLog(t0 + 23, "m1", "-", "-", null, 1, null, null,
                    "REJECTED", 0, null, "no such model", "claude-code", null, null,
                    404, "MODEL_NOT_SUPPORTED", null);
            inserted++;
            // 窗口外（次日）：不许被算进今天
            dao.insertRequestLog(toMs + 3_600_000L, "m1", "alpha", "m1", 700, 800, 1L, 1L,
                    "OK", 1, null, null, "claude-code", null, null, 200, null, "k1");
            inserted++;

            // asyncWrite 是排队写：轮询到看见为止，不拿"命令退出码"当成功
            long seen = 0;
            for (int i = 0; i < 200 && (seen = countAll(db)) != inserted; i++) Thread.sleep(50);
            check("造数全部落库（异步写队列已排空）", seen == inserted,
                    "seen=" + seen + " inserted=" + inserted);

            List<Map<String, Object>> all = dao.keyUsage(fromMs, toMs, List.of(), List.of(), List.of());

            // ① 每把 Key 各自成行
            Map<String, Object> a1k1 = row(all, "alpha", "m1", "k1");
            Map<String, Object> a1k2 = row(all, "alpha", "m1", "k2");
            check("alpha/m1/k1 独立成行", a1k1 != null);
            check("alpha/m1/k2 独立成行", a1k2 != null);
            if (a1k1 != null) {
                checkEq("k1 请求数（3 成功 + 1 掐断；窗口外那条不含）", 4, num(a1k1, "requests"));
                checkEq("k1 成功数", 3, num(a1k1, "ok"));
                checkEq("k1 失败数", 1, num(a1k1, "failures"));
                checkEq("k1 掐断细分计入 timeouts（与 failuresByClient 同口径）", 1, num(a1k1, "timeouts"));
                checkEq("k1 未撞 429", 0, num(a1k1, "rateLimits"));
                checkEq("k1 输入 token", 30, num(a1k1, "tokensIn"));
                checkEq("k1 输出 token", 15, num(a1k1, "tokensOut"));
            }
            if (a1k2 != null) {
                checkEq("k2 请求数", 2, num(a1k2, "requests"));
                checkEq("k2 成功数", 1, num(a1k2, "ok"));
                checkEq("k2 撞 429 按 error_class 计（与 status 取值无关）", 1, num(a1k2, "rateLimits"));
                checkEq("k2 报错细分", 1, num(a1k2, "errors"));
            }

            // ② 无归属桶单独成行，且不摊给在用的 Key
            Map<String, Object> a1none = row(all, "alpha", "m1", "");
            check("存在独立的\"上线前无归属\"行（channel=\"\"）", a1none != null);
            if (a1none != null) checkEq("无归属行 == 3（2 条 NULL + 1 条空串归一）", 3, num(a1none, "requests"));
            checkEq("alpha/m1 只有 3 个桶（k1、k2、无归属）：空串没和 NULL 分家", 3,
                    all.stream().filter(r -> "alpha".equals(r.get("provider"))
                            && "m1".equals(r.get("model"))).count());

            // ③ 拒绝行不进表
            boolean hasDash = all.stream().anyMatch(r -> "-".equals(String.valueOf(r.get("provider"))));
            check("provider_id='-' 的网关拒绝行不进按 Key 表", !hasDash);

            // 中位数：只取成功行；奇数取中间、偶数取中间两值均值
            checkEq("k1 TTFT 中位数=200（3 个成功样本的中间值；掐断行不是成功样本）",
                    200L, a1k1 == null ? -1L : num(a1k1, "ttftMedianMs"));
            Map<String, Object> a4k1 = row(all, "alpha", "m4", "k1");
            checkEq("偶数样本取中间两值均值（100/300 → 200）",
                    200L, a4k1 == null ? -1L : num(a4k1, "ttftMedianMs"));

            // 窗口边界：只取明天那一天的窗口应当只有那条"明天"的行
            List<Map<String, Object>> tomorrow = dao.keyUsage(toMs + 1, Long.MAX_VALUE / 2,
                    List.of(), List.of(), List.of());
            Map<String, Object> tmRow = row(tomorrow, "alpha", "m1", "k1");
            check("窗口外那条落在明天窗口里（边界按落库时间，没串台）", tmRow != null);
            checkEq("明天窗口只有那 1 条", 1L, tmRow == null ? -1L : num(tmRow, "requests"));

            // 过滤条件不许被悄悄丢掉：供应商 / 模型 / 来源工具
            List<Map<String, Object>> onlyBeta = dao.keyUsage(fromMs, toMs, List.of("beta"), List.of(), List.of());
            check("供应商过滤生效（只剩 beta）",
                    !onlyBeta.isEmpty() && onlyBeta.stream().allMatch(r -> "beta".equals(r.get("provider"))),
                    "rows=" + onlyBeta.size());
            List<Map<String, Object>> onlyM2 = dao.keyUsage(fromMs, toMs, List.of(), List.of(), List.of("m2"));
            check("模型过滤生效（只剩 alpha/m2）",
                    onlyM2.size() == 1 && "m2".equals(onlyM2.get(0).get("model")), "rows=" + onlyM2);
            List<Map<String, Object>> onlyCompress = dao.keyUsage(fromMs, toMs, List.of(),
                    List.of("gw-compress"), List.of());
            long cReq = onlyCompress.stream().mapToLong(r -> num(r, "requests")).sum();
            check("来源工具过滤生效（只剩 gw-compress 那 2 发：1 条无归属 + 1 条 m2/k1）",
                    cReq == 2 && onlyCompress.size() == 2, "rows=" + onlyCompress.size() + " req=" + cReq);
            // 无归属行在按来源工具过滤时仍然单列，不因过滤而消失或换桶
            check("按工具过滤后无归属行仍单独成行", row(onlyCompress, "alpha", "m1", "") != null);

            // 组合过滤：供应商 + 模型同时给，两个条件都要生效
            List<Map<String, Object>> combo = dao.keyUsage(fromMs, toMs, List.of("alpha"), List.of(), List.of("m4"));
            check("供应商×模型组合过滤只剩 alpha/m4",
                    combo.size() == 1 && "m4".equals(combo.get(0).get("model")), "rows=" + combo);

            check("全窗口分组数 = 6（alpha/m1×k1,k2,\"\", alpha/m2×k1, alpha/m4×k1, beta/m1×k1）",
                    all.size() == 6, "rows=" + all.stream().map(r -> r.get("provider") + "/"
                            + r.get("model") + "/" + r.get("channel")).toList());
        } finally {
            deleteDir(dir);
        }
        System.out.println(failures == 0 ? "\nALL PASS" : "\nFAILURES=" + failures);
        if (failures > 0) System.exit(1);
    }

    static long countAll(Database db) throws java.sql.SQLException {
        Long v = db.queryOne("SELECT COUNT(*) FROM request_log", rs -> rs.getLong(1));
        return v == null ? -1 : v;
    }

    static void deleteDir(Path dir) {
        try (var walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignore) { }
            });
        } catch (Exception ignore) {
        }
    }
}
