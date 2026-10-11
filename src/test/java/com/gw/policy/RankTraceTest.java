package com.gw.policy;

import com.gw.config.ConfigService;
import com.gw.store.Database;
import com.gw.store.StatsDao;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * 评分留痕单测（main 直跑）：验证 RankTrace 抓到的东西确实是路由真正用的东西——
 *  ① 因子分解连乘 == 最终分（界面解释"这一分怎么来的"不能是自编故事）；
 *  ② 四类门控（渠道禁用/临时停用/档位不符/上下文超限）都要留下出局原因，不能静默消失；
 *  ③ 抽签席份额 = √(score/max) 归一化，且席内份额和为 1（分摊对照的"应得"侧要靠它）；
 *  ④ 评分沙盘预览只读：绝不占用半开探测槽（占了就把真实恢复探测推迟了）。
 * 用临时 SQLite + 临时 YAML，不碰真实 config.yaml 与 data/ 库。
 */
public class RankTraceTest {

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
        boolean ok = expected == null ? actual == null : expected.equals(actual);
        System.out.println((ok ? "PASS " : "FAIL ") + name + (ok ? "" : "  期望=" + expected + " 实际=" + actual));
        if (!ok) failures++;
    }

    /** 界面要连乘校验的因子键（不含 ttftEstMs/priority 等诊断附值） */
    static final List<String> FACTOR_KEYS = List.of(
            "P_priority", "H_health", "Q_quality", "S_ttft", "T_throughput", "C_costTier",
            "K_category", "CS_codeScore", "U_userWeight", "R_rateRecover", "L_ratePressure",
            "TW_toolWeak", "ES_earlyStop");

    static final String TEST_YAML = """
            strategy: auto
            gateway-keys:
              - test-key
            defaults:
              loadShare: true
              shareBandSize: 3
              shareSlowTtftMs: 6000
            providers:
              - id: alpha
                name: alpha
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [a1, a2]
                channels:
                  - id: k1
                    api-key: x
                  - id: k2
                    api-key: y
                    enabled: false
              - id: beta
                name: beta
                base-url: http://127.0.0.1:9/v1
                priority: 2
                default-cost-tier: free
                static-models: [b1]
                channels:
                  - id: k1
                    api-key: x
              - id: gamma
                name: gamma
                base-url: http://127.0.0.1:9/v1
                priority: 3
                default-cost-tier: premium
                static-models: [g1, tiny]
                channels:
                  - id: k1
                    api-key: x
            routing-overrides:
              - provider-id: gamma
                model: tiny
                context-window: 1000
            """;

    static List<PolicyService.Candidate> allCandidates() {
        return List.of(
                new PolicyService.Candidate("alpha", "a1", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a1", "k2", 1, null),   // 渠道已禁用
                new PolicyService.Candidate("alpha", "a2", "k1", 1, null),
                new PolicyService.Candidate("beta", "b1", "k1", 1, null),
                new PolicyService.Candidate("gamma", "g1", "k1", 1, null),
                new PolicyService.Candidate("gamma", "tiny", "k1", 1, null));  // 上下文 1000
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("gw-ranktrace");
        Path cfgFile = dir.resolve("config.yaml");
        Files.writeString(cfgFile, TEST_YAML);
        try (Database db = new Database(dir.resolve("t.db").toString(), cfgFile.toString())) {
            ConfigService cs = new ConfigService(db);
            Field f = ConfigService.class.getDeclaredField("configPath");
            f.setAccessible(true);
            f.set(cs, cfgFile.toString());
            cs.init();
            StatsDao dao = new StatsDao(db);
            PolicyService ps = new PolicyService(cs, dao, null);

            factorProductMatchesScore(ps);
            gateReasonsAreRecorded(ps);
            shareIsNormalized(ps);
            previewDoesNotClaimHalfOpen(cs, ps);
            usageSpansAllClients(db, cs);
            shareComesFromDbNotJustMemory(db, cs, ps);
            keyRequestCounterTracksEntry(ps);
        } finally {
            deleteDir(dir);
        }
        System.out.println(failures == 0 ? "\nALL PASS" : "\nFAILURES=" + failures);
        if (failures > 0) System.exit(1);
    }

    /** ① 因子连乘 == 分数：界面逐因子展示的乘积必须能还原出路由真正用的那个分 */
    static void factorProductMatchesScore(PolicyService ps) {
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        t.requestedModel = "any";
        List<PolicyService.Scored> ranked = ps.rank(allCandidates(), "auto", false, null, null, false, null, t);
        check("留痕行数 == 参与评分的候选数", t.rows.size() == 5);   // k2 被门控剔除，不评分
        double worst = 0;
        for (PolicyService.RankTrace.Row r : t.rows) {
            check(r.provider + "/" + r.model + " 有因子分解", r.factors != null && r.factors.size() >= FACTOR_KEYS.size());
            double prod = 1.0;
            for (String k : FACTOR_KEYS) {
                Double v = r.factors.get(k);
                if (v == null) { check("因子缺失: " + k, false); return; }
                prod *= v;
            }
            worst = Math.max(worst, Math.abs(prod - r.score) / Math.max(1e-9, Math.abs(r.score)));
        }
        check("全部候选：因子连乘 == 最终分（相对误差 <1e-9）", worst < 1e-9);
        check("排序结果非空且首位被标主选", !ranked.isEmpty() && pickedCount(t) == 1);
        check("主选 == 结果首位", ranked.get(0).candidate.providerId().equals(t.pickedProvider)
                && ranked.get(0).candidate.model().equals(t.pickedModel));
        check("诊断附值齐（ttftEstMs/priority/失败数）",
                t.rows.get(0).factors.containsKey("ttftEstMs")
                        && t.rows.get(0).factors.containsKey("priority")
                        && t.rows.get(0).factors.containsKey("consecutiveFailures"));
    }

    /** ② 四类门控都要留名——"为什么没分摊到我"最常见的答案就在这里，不能是静默 continue */
    static void gateReasonsAreRecorded(PolicyService ps) {
        ps.setTempDisabled("beta", "b1", System.currentTimeMillis() + 60_000);
        // 档位过滤排在超窗过滤之前：gamma/tiny（premium 且限 1000 窗）在 free-only 下先被档位挡下，
        // 所以超窗原因要用 auto 策略单独验一次——门控顺序本身就是行为契约的一部分。
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        ps.rank(allCandidates(), "free-only", false, null, null, false, null, t);
        ps.clearTempDisabled("beta", "b1");
        PolicyService.RankTrace t2 = new PolicyService.RankTrace();
        ps.rank(allCandidates(), "auto", false, null, 5000, false, null, t2);

        Map<String, String> byReason = new HashMap<>();
        for (PolicyService.RankTrace.Row r : t.filtered) byReason.put(r.reason, r.note);
        for (PolicyService.RankTrace.Row r : t2.filtered) byReason.putIfAbsent(r.reason, r.note);
        check("渠道禁用 → CHANNEL_DISABLED", byReason.containsKey("CHANNEL_DISABLED"));
        check("渠道禁用的 note 说清是谁", byReason.getOrDefault("CHANNEL_DISABLED", "").contains("渠道已禁用"));
        check("临时停用 → TEMP_DISABLED", byReason.containsKey("TEMP_DISABLED"));
        check("档位不符 → TIER_BLOCKED", byReason.containsKey("TIER_BLOCKED"));
        check("档位 note 点名 premium", byReason.getOrDefault("TIER_BLOCKED", "").contains("premium"));
        check("超窗 → CONTEXT_TOO_LARGE", byReason.containsKey("CONTEXT_TOO_LARGE"));
        check("超窗 note 带上 5000 与 1000", byReason.getOrDefault("CONTEXT_TOO_LARGE", "")
                .contains("5000") && byReason.getOrDefault("CONTEXT_TOO_LARGE", "").contains("1000"));
        check("被门控的候选不进 rows（没打分就是没分）",
                t.rows.stream().noneMatch(r -> r.provider.equals("gamma") && r.model.equals("tiny")));
        check("门控候选不会冒充排序结果", t.rows.stream().noneMatch(r -> "FILTERED".equals(r.state)));
        // 档位门控优先于超窗门控：free-only 下 gamma/tiny 报的是档位，不是窗口
        check("门控按顺序命中（先档位后超窗）", t.filtered.stream()
                .anyMatch(r -> r.reason.equals("TIER_BLOCKED") && r.model.equals("tiny")));
    }

    /** ③ 抽签席份额 = √(score/max) 归一化，席内合计 1 —— 分摊对照的"应得"一侧靠它 */
    static void shareIsNormalized(PolicyService ps) {
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        t.loadShare = true;
        ps.rank(allCandidates(), "auto", false, null, null, false, null, t);
        List<PolicyService.RankTrace.Row> band = new ArrayList<>();
        for (PolicyService.RankTrace.Row r : t.rows) if (r.inBand) band.add(r);
        check("抽签席按模型去重（同模型多 Key 只占一席）", band.size() == t.bandSize
                || band.size() < t.rows.size());
        double sum = 0;
        for (PolicyService.RankTrace.Row r : band) sum += r.share;
        check("席内份额和 == 1（" + sum + "）", Math.abs(sum - 1.0) < 1e-9);
        double max = band.stream().mapToDouble(r -> r.score).max().orElse(0);
        boolean ok = true;
        for (PolicyService.RankTrace.Row r : band) {
            double w = Math.sqrt(Math.max(0.0, r.score) / Math.max(max, 1e-9));
            double tot = band.stream().mapToDouble(b -> Math.sqrt(Math.max(0.0, b.score) / Math.max(max, 1e-9))).sum();
            if (Math.abs(r.share - w / tot) > 1e-9) ok = false;
        }
        check("每席份额 == √(score/max) 归一化", ok);
        check("未进席的行份额记 0", t.rows.stream().filter(r -> !r.inBand).allMatch(r -> r.share == 0));
        check("留痕自带抽签参数（界面要说明规则）",
                t.loadShare && t.bandSize == 3 && t.slowTtftMs == 6000);
    }

    /** ④ 预览只读：冷却到期后的半开探测槽，沙盘不能占（占了真实恢复探测就被推迟一轮） */
    static void previewDoesNotClaimHalfOpen(ConfigService cs, PolicyService ps) throws Exception {
        PolicyService.EntryRuntime er = ps.entry("alpha", "a1");
        er.consecutiveFailures = 3;
        er.coolDownUntil = System.currentTimeMillis() - 1000;   // 冷却已到期 → 进入半开窗口
        er.halfOpenProbe = true;
        er.halfOpenBusy = false;

        List<PolicyService.Candidate> only = List.of(new PolicyService.Candidate("alpha", "a1", "k1", 1, null));
        PolicyService.RankTrace prev = new PolicyService.RankTrace();
        prev.preview = true;
        ps.rank(only, "auto", false, null, null, false, null, prev);
        check("预览仍把半开中的候选按冷却呈现", !prev.rows.isEmpty() && "COOLING".equals(prev.rows.get(0).state));
        check("预览说明是半开槽被占", prev.rows.get(0).note != null && prev.rows.get(0).note.contains("半开"));
        check("预览未占用半开槽", !er.halfOpenBusy);

        ps.rank(only, "auto", false, null, null, false, null, null);
        check("真实请求照样能抢到半开槽", er.halfOpenBusy);
    }

    static long pickedCount(PolicyService.RankTrace t) {
        return t.rows.stream().filter(r -> r.picked).count();
    }

    /** 取某 provider×model 的请求数（没有该行返回 -1，绝不"取第一条"回退） */
    static long reqOf(List<Map<String, Object>> rows, String provider, String model) {
        for (Map<String, Object> r : rows)
            if (provider.equals(r.get("provider")) && model.equals(r.get("model")))
                return ((Number) r.get("requests")).longValue();
        return -1;
    }

    /** ⑤ 实际用量口径：跨全部来源工具汇总。
     * 旧实现带 {@code AND client = ''}（usage_daily 加 client 维度时给老数据回填的空串），
     * 于是 agent 打来的流量全被滤掉，界面只显示出实际量的三成——"没分摊"的结论被严重低估。
     * 用一个孤立的历史日期建数据，避免与库里其它天的行相互污染；写入走真实异步队列，
     * 所以判定要轮询到看见为止，不能拿"提交没报错"当成功。 */
    static void usageSpansAllClients(Database db, ConfigService cs) throws Exception {
        RankTraceStore store = new RankTraceStore(db, cs);
        StatsDao dao = new StatsDao(db);
        String day = "2020-01-02";
        dao.upsertUsage(day, "alpha", "a1", "", 5, 100, 50);
        dao.upsertUsage(day, "alpha", "a1", "claude-code", 40, 200, 120);
        dao.upsertUsage(day, "alpha", "a1", "gw-compress", 7, 900, 60);
        dao.upsertUsage(day, "beta", "b1", "claude-code", 30, 300, 200);

        List<Map<String, Object>> all = await(() -> store.actualUsage(day, day, null),
                rs -> rs.size() >= 2);
        check("全部来源汇总：alpha/a1 = 5+40+7 = 52（旧口径只算得到 5）", reqOf(all, "alpha", "a1") == 52,
                "实际=" + reqOf(all, "alpha", "a1"));
        check("全部来源汇总：beta/b1 = 30", reqOf(all, "beta", "b1") == 30, "");
        List<Map<String, Object>> one = store.actualUsage(day, day, "claude-code");
        check("按来源过滤：alpha/a1 只剩 claude-code 的 40", reqOf(one, "alpha", "a1") == 40,
                "实际=" + reqOf(one, "alpha", "a1"));
        check("按来源过滤：beta/b1 的 30 也属于 claude-code", reqOf(one, "beta", "b1") == 30, "");
        check("只看 gw-compress 时不含 beta", store.actualUsage(day, day, "gw-compress").stream()
                .allMatch(r -> "alpha".equals(r.get("provider"))), "");
        List<String> clients = store.usageClients(day, day);
        check("来源列表齐（界面下拉的数据源）", clients.contains("claude-code") && clients.contains("gw-compress"),
                "clients=" + clients);
    }

    /** 轮询到满足条件为止（异步写队列要给它时间落库），最多 5 秒 */
    static <T> T await(java.util.function.Supplier<T> get, java.util.function.Predicate<T> ready) throws Exception {
        T v = null;
        for (int i = 0; i < 100; i++) {
            v = get.get();
            if (ready.test(v)) return v;
            Thread.sleep(50);
        }
        return v;
    }

    /**
     * ⑥ 应得份额来自采样库，不只是内存环形。
     * 旧实现只遍历内存（最近 200 次、重启即清），网关一重启整列"暂无观测"，
     * 而库里的采样明明还在。这里落库后换一个全新 store 实例（等价于重启）来验。
     * 顺带验未进席原因码的分类计数（界面"为什么没摊到我"靠它）。
     */
    static void shareComesFromDbNotJustMemory(Database db, ConfigService cs, PolicyService ps) throws Exception {
        cs.persist(cfg -> cfg.getDefaults().setRankTraceSampleRate(1.0));   // 本轮全落库，测试不靠运气
        RankTraceStore store = new RankTraceStore(db, cs);
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        t.traceId = "T-share-1";
        t.requestedModel = "any";
        t.client = "claude-code";
        t.strategy = "auto";
        t.loadShare = true;
        ps.rank(allCandidates(), "auto", false, null, null, false, null, t);
        t.ts = System.currentTimeMillis();
        store.commit(t);

        long from = t.ts - 60_000, to = t.ts + 600_000;
        // asyncWrite 是排队写：轮询到看见为止，不拿"管道退出码"当成功
        int seen = 0;
        for (int i = 0; i < 100 && (seen = store.observeShare(from, to, null, null).samples()) == 0; i++)
            Thread.sleep(50);
        check("决策快照已落采样库", seen >= 1, "samples=" + seen);

        RankTraceStore afterRestart = new RankTraceStore(db, cs);   // 内存空手，只剩库
        com.gw.policy.RankTraceStore.ShareObservation obs = afterRestart.observeShare(from, to, null, null);
        List<PolicyService.RankTrace.Row> band = new ArrayList<>();
        for (PolicyService.RankTrace.Row r : t.rows) if (r.inBand) band.add(r);
        check("重启后仍读得到应得份额（来自库，不是内存）",
                obs.samples() >= 1 && obs.fromDb() >= 1 && obs.fromMem() == 0,
                "samples=" + obs.samples() + " db=" + obs.fromDb() + " mem=" + obs.fromMem());
        boolean shareOk = true;
        for (PolicyService.RankTrace.Row r : band) {
            Double got = obs.share().get(r.provider + "\n" + r.model);
            if (got == null || Math.abs(got - r.share) > 1e-3) shareOk = false;   // 单样本：均值就该等于它本身
        }
        check("库内 payload 解析出的份额 == 当时席内份额", shareOk && !band.isEmpty(),
                "band=" + band.size() + " share=" + obs.share());
        check("同一条决策不重复计（内存与库去重）",
                store.observeShare(from, to, null, null).samples() == obs.samples(),
                "mem+db=" + store.observeShare(from, to, null, null).samples() + " db=" + obs.samples());
        check("未进席的行带原因码（候选 5 个模型 > 席 3）",
                t.rows.stream().filter(r -> !r.inBand && "OK".equals(r.state))
                        .allMatch(r -> r.bandOut != null), "");
        check("未进席原因分类计数有数（界面按码汇总，不猜文案）",
                !obs.bandOut().isEmpty() && obs.bandOut().values().stream().mapToInt(Integer::intValue).sum() > 0,
                "bandOut=" + obs.bandOut());
        check("按来源过滤：claude-code 有样本、别家来源为 0",
                obs.samples() > 0 && afterRestart.observeShare(from, to, "other-tool", null).samples() == 0, "");
    }

    /** ⑦ Key 维度的"今日请求数"必须与 provider×model 维度同起同落。
     *  曾经只给 Key 记 token、不记请求：界面就会摆出"今日 0 次 · 83 token"这种自相矛盾的数——
     *  两个数来自同一个计数器对象，读的人无法判断哪个是假的。 */
    static void keyRequestCounterTracksEntry(PolicyService ps) {
        long e0 = ps.requestsToday("alpha", "a1"), k0 = ps.requestsTodayChannel("alpha", "k1");
        ps.recordSuccess("alpha", "a1", "k1", 120L, 10L, 5L, false, "claude-code", 300L);
        long eD = ps.requestsToday("alpha", "a1") - e0, kD = ps.requestsTodayChannel("alpha", "k1") - k0;
        check("成功一发：provider×model 与 Key 两个维度各 +1", eD == 1 && kD == 1,
                "entry+" + eD + " key+" + kD);

        long e1 = ps.requestsToday("alpha", "a1"), k1 = ps.requestsTodayChannel("alpha", "k1");
        ps.recordFailure("alpha", "a1", "k1", 500, "boom", "claude-code");
        eD = ps.requestsToday("alpha", "a1") - e1;
        kD = ps.requestsTodayChannel("alpha", "k1") - k1;
        check("失败一发也按 Key 计一次（失败不烧 token，但请求数不能漏）", eD == 1 && kD == 1,
                "entry+" + eD + " key+" + kD);

        // 无 Key 归属（网关侧拒绝：从未选路）：不许凭空摊给某把 Key
        long k2Before = ps.requestsTodayChannel("alpha", "k2"), a2Before = ps.requestsToday("alpha", "a2");
        ps.addRequestCount("alpha", "a2", null, "claude-code");
        check("channelId=null 只进 provider×model 维度，不落到任何 Key 上",
                ps.requestsToday("alpha", "a2") - a2Before == 1 && ps.requestsTodayChannel("alpha", "k2") == k2Before,
                "a2+" + (ps.requestsToday("alpha", "a2") - a2Before) + " k2=" + ps.requestsTodayChannel("alpha", "k2"));

        // 探测背景流量：两个维度都不进（否则"今日量"被 probe 灌水，与真实用量对不上）
        long e2 = ps.requestsToday("alpha", "a1"), k3 = ps.requestsTodayChannel("alpha", "k1");
        ps.recordFailure("alpha", "a1", "k1", 500, "boom", "probe");
        check("probe 失败不进任何维度的请求数",
                ps.requestsToday("alpha", "a1") == e2 && ps.requestsTodayChannel("alpha", "k1") == k3,
                "entry=" + ps.requestsToday("alpha", "a1") + "/" + e2
                        + " key=" + ps.requestsTodayChannel("alpha", "k1") + "/" + k3);
    }

    static void deleteDir(Path dir) {
        try {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignore) {}
            });
        } catch (Exception ignore) {}
    }
}
