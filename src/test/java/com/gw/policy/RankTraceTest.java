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
 *  ③ 抽签席份额 = √(偏好分/席内最大) 归一，且席内份额和为 1（分摊对照的"应得"侧要靠它）；
 *  ④ 评分沙盘预览只读：绝不占用半开探测槽（占了就把真实恢复探测推迟了）；
 *  ⑤ 速度只决定席位、偏好决定份额（"相差不是很大就该分摊"这条要求的钉子）；
 *  ⑥ 过期的跨重启中位数会被日志重算顶掉，测量伪值/巡检流量不进速度历史；
 *  ⑦ 运行时快照的两个键（渠道标记逐次累积、模型名大小写分裂）写干净、历史脏行清得掉；
 *  ⑧ 出字速度（T）也只进分数、不进份额（旧口径只除 S，吞吐差 8 倍照样把流量吸走）；
 *  ⑨ 供应商级封顶：席位封顶（甲·一家最多 N 席，超出的同家模型撤席但仍兜底）与
 *    份额封顶（乙·一家合计不超过 capPct，余量按权重回灌），两条都要能算准、也能在留痕里说清；
 *  ⑩ 采样库聚合的分母是"有席的决策数"：模型级与供应商级同一分母，供应商级加总才等于 100%。
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

    /** 失败现场：逐行给出席位/原因码/速度状态/文案，判"为什么没摊到我"不靠猜 */
    static String dbg(PolicyService.RankTrace t) {
        StringBuilder sb = new StringBuilder("seats=").append(t.seatsStrict).append('+')
                .append(t.seatsRelaxed).append('+').append(t.seatsExplore).append(" rows=[");
        for (PolicyService.RankTrace.Row r : t.rows) {
            sb.append(r.provider).append('/').append(r.model).append('{')
                    .append("in=").append(r.inBand)
                    .append(",out=").append(r.bandOut)
                    .append(",st=").append(r.state)
                    .append(",sp=").append(r.speedState)
                    .append(",note=").append(r.note == null ? "null"
                            : r.note.substring(0, Math.min(20, r.note.length())))
                    .append("} ");
        }
        return sb.append(']').toString();
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
              shareSpeedTieRatio: 3
              # ③⑤ 两条用例只验"份额 = √(偏好分/max) 归一"，供应商封顶会改写份额，这里先关掉；
              # 封顶本身由 ⑨ 单独验（那一条再把 2 席 / 35% 写回来）
              shareProviderSeatCap: 0
              shareProviderCapPct: 0
              # ⑤ 那条测的是"掉出并列带就撤席"，而席位下限会把带外最快的家再补回来（同一条断言两种规则）；
              # 补席本身由 ShareSeatFloorTest 单独钉，这里显式关掉，两条用例才各测一件事。
              shareMinSeats: 0
              # 同上：探索测量席会把「没被有效测过」的候选补进席，而本套用例要钉的是
              # "带内 3 席 + 未进席原因计数"——探索席由 SpeedSampleFreshnessTest 单独钉。
              shareExploreSeats: 0
              chronicRateMinSamples: 8
              chronicSeatBlockRatio: 0.3
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
            shareDenominatorIsDecisionsNotRows(db, cs, ps);
            keyRequestCounterTracksEntry(ps);
            speedBuysSeatNotShare(ps);
            throughputIsDividedOutOfShare(ps);
            cappedSharesMath();
            throughputFactorValues();
            providerSeatCapAndShareCap(cs, cfgFile, ps);
            staleRestoredMedianIsReplacedByLog(db, cs);
            snapshotKeysStayClean(db, cs, ps);
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

    /** ③ 抽签席份额 = √(偏好分/席内最大偏好分) 归一，席内合计 1 —— 分摊对照的"应得"一侧靠它。
     *  偏好分 = 综合分 ÷ S 因子：速度只决定"能不能进席"，进席后接几成流量由偏好说话。 */
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
        double max = band.stream().mapToDouble(RankTraceTest::prefOf).max().orElse(0);
        boolean ok = true;
        for (PolicyService.RankTrace.Row r : band) {
            double w = Math.sqrt(Math.max(0.0, prefOf(r)) / Math.max(max, 1e-9));
            double tot = band.stream().mapToDouble(b ->
                    Math.sqrt(Math.max(0.0, prefOf(b)) / Math.max(max, 1e-9))).sum();
            if (Math.abs(r.share - w / tot) > 1e-9) ok = false;
        }
        check("每席份额 == √(偏好分/max) 归一", ok);
        check("未进席的行份额记 0", t.rows.stream().filter(r -> !r.inBand).allMatch(r -> r.share == 0));
        check("留痕自带抽签参数（界面要说明规则）",
                t.loadShare && t.bandSize == 3 && t.slowTtftMs == 6000 && t.speedTieRatio == 3);
    }

    /** 份额用的偏好分：留痕行自带的两个速度因子（S 首字、T 出字）把速度维整体除回去（与服务端 prefScore 同口径）。
     *  测试故意从"界面能看见的因子"反推，而不是重新实现一遍服务端公式——
     *  因子表若与分数脱节，① 号用例已经先红。 */
    static double prefOf(PolicyService.RankTrace.Row r) {
        double p = r.score;
        for (String speedKey : new String[]{"S_ttft", "T_throughput"}) {
            Double v = r.factors == null ? null : r.factors.get(speedKey);
            p = p / Math.max(v == null ? 1.0 : v, 1e-9);
        }
        return p;
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
        // asyncWrite 是排队写：要轮询到「库」里看见为止。
        // 拿带内存环的 store 轮询等于没等——刚 commit 那条立刻在内存里可见，循环当场退出，
        // 异步写还没落库，下一行换成只认库的实例就采到 0 条（2026-10-16 的随机失败就是这个形状）。
        RankTraceStore afterRestart = new RankTraceStore(db, cs);   // 内存空手，只剩库
        com.gw.policy.RankTraceStore.ShareObservation obs =
                afterRestart.observeShare(from, to, null, null);
        for (int i = 0; i < 100 && obs.samples() == 0; i++) {
            Thread.sleep(50);
            obs = afterRestart.observeShare(from, to, null, null);
        }
        check("决策快照已落采样库", obs.samples() >= 1, "samples=" + obs.samples());
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
                        .allMatch(r -> r.bandOut != null), dbg(t));
        check("未进席原因分类计数有数（界面按码汇总，不猜文案）",
                !obs.bandOut().isEmpty() && obs.bandOut().values().stream().mapToInt(Integer::intValue).sum() > 0,
                "bandOut=" + obs.bandOut() + " " + dbg(t));
        check("按来源过滤：claude-code 有样本、别家来源为 0",
                obs.samples() > 0 && afterRestart.observeShare(from, to, "other-tool", null).samples() == 0, "");
    }

    /**
     * ⑩ 份额均值的分母是"有席的决策数"，模型级与供应商级共用同一个分母。
     * 旧口径给每个模型在"它自己进席的那几次"里取均值，界面把同家几个模型相加当"这家的应得"，
     * 实测能加到 169%——封顶线就画在这一层，读的人只会更糊涂。
     * 这里手工摆两次决策（各家份额已知），一次三家、一次两家，把三条性质钉住：
     * 供应商级加总＝100%、只进过一次席的模型按两次平均、一条无席决策不污染分母。
     */
    static void shareDenominatorIsDecisionsNotRows(Database db, ConfigService cs, PolicyService ps) throws Exception {
        cs.persist(cfg -> cfg.getDefaults().setRankTraceSampleRate(1.0));
        RankTraceStore store = new RankTraceStore(db, cs);
        // 本用例只测聚合口径，用专属 client 把自己和同库其他用例的决策隔开：
        // 其他用例也在"现在"落一条有席决策，时间窗一模就撞车，分母会被悄悄抬高（第一次跑就是这么坏的）
        String cl = "share-denominator-" + System.nanoTime();
        long base = System.currentTimeMillis();

        PolicyService.RankTrace t1 = seatTrace("T-dec-1", base - 30_000, cl);
        addSeat(t1, "alpha", "a1", 0.5);
        addSeat(t1, "alpha", "a2", 0.3);
        addSeat(t1, "beta", "b1", 0.2);
        store.commit(t1);

        PolicyService.RankTrace t2 = seatTrace("T-dec-2", base - 20_000, cl);
        addSeat(t2, "alpha", "a1", 0.7);
        addSeat(t2, "gamma", "g1", 0.3);
        store.commit(t2);

        // 无席决策（分摊开着但候选全被门槛撤席）：对"谁该接几成"没有表态，不该进分母
        PolicyService.RankTrace t3 = seatTrace("T-dec-3", base - 10_000, cl);
        PolicyService.RankTrace.Row x = new PolicyService.RankTrace.Row();
        x.provider = "delta"; x.model = "d1"; x.inBand = false;
        x.state = "OK"; x.bandOut = PolicyService.BAND_SLOW;
        t3.rows.add(x);
        store.commit(t3);

        long from = base - 120_000, to = base + 60_000;
        RankTraceStore fresh = new RankTraceStore(db, cs);          // 等价重启：只认库
        com.gw.policy.RankTraceStore.ShareObservation obs = fresh.observeShare(from, to, cl, null);
        for (int i = 0; i < 100 && obs.seatedDecisions() < 2; i++) {
            Thread.sleep(50);
            obs = fresh.observeShare(from, to, cl, null);
        }
        check("两次有席决策都被数到（无席那条不进分母）", obs.seatedDecisions() == 2,
                "seated=" + obs.seatedDecisions() + " samples=" + obs.samples());

        // 有效封顶线＝每次决策 max(配置值, 1÷该次占席家数) 再取均值：两条决策各 2 家 → 都是 0.5
        Double eff = obs.providerCapPctEffective();
        check("有效封顶线按占席家数放宽（配置 0.35、每次 2 家占席 → 0.5，界面不能拿 0.35 标红 40%）",
                eff != null && near(eff, 0.5), "eff=" + eff);

        double pSum = obs.providerShare().values().stream().mapToDouble(Double::doubleValue).sum();
        check("供应商级应得份额加总＝100%（旧口径会把模型级均值相加算出 169%）",
                Math.abs(pSum - 1.0) < 1e-6, "pSum=" + pSum + " prov=" + obs.providerShare());
        check("alpha 两次都在席：(0.8+0.7)/2=0.75",
                near(obs.providerShare().getOrDefault("alpha", -1.0), 0.75), "alpha=" + obs.providerShare().get("alpha"));
        check("beta 只在第一次有席：0.2/2=0.1（按出现过的次数平均会错报 0.2）",
                near(obs.providerShare().getOrDefault("beta", -1.0), 0.1), "beta=" + obs.providerShare().get("beta"));
        check("gamma 只在第二次有席：0.3/2=0.15",
                near(obs.providerShare().getOrDefault("gamma", -1.0), 0.15), "gamma=" + obs.providerShare().get("gamma"));
        check("模型级与供应商级同一分母：alpha/a1=(0.5+0.7)/2=0.6",
                near(obs.share().getOrDefault("alpha\na1", -1.0), 0.6), "a1=" + obs.share().get("alpha\na1"));
        check("模型级 alpha 两个模型合计＝供应商级 alpha（界面合计行要能对上）",
                near(obs.share().getOrDefault("alpha\na1", 0.0) + obs.share().getOrDefault("alpha\na2", 0.0),
                        obs.providerShare().getOrDefault("alpha", -1.0)),
                "sum=" + (obs.share().getOrDefault("alpha\na1", 0.0) + obs.share().getOrDefault("alpha\na2", 0.0))
                        + " prov=" + obs.providerShare().get("alpha"));
        check("未进席原因计数仍从那条无席决策里收到",
                obs.bandOut().getOrDefault(PolicyService.BAND_SLOW, 0) >= 1, "bandOut=" + obs.bandOut());
    }

    /** 造一条"分摊生效且真有席"的决策壳（份额手工给，用例只测聚合口径不测抽签） */
    static PolicyService.RankTrace seatTrace(String traceId, long ts, String client) {
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        t.traceId = traceId;
        t.requestedModel = "any";
        t.client = client;
        t.strategy = "auto";
        t.loadShare = true;
        t.providerSeatCap = 2;
        t.providerCapPct = 0.35;
        t.ts = ts;
        return t;
    }

    static void addSeat(PolicyService.RankTrace t, String provider, String model, double share) {
        PolicyService.RankTrace.Row r = new PolicyService.RankTrace.Row();
        r.provider = provider; r.model = model; r.channel = "k1";
        r.inBand = true; r.share = share; r.state = "OK";
        t.rows.add(r);
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

    /** ⑧ 速度只买"席位"、不买"份额"（本轮口径分工，也是"响应速度相差不是很大就该分摊"的钉子）。
     *  a1 实测 400ms、a2 实测 1500ms：差 3.75 倍但只差 1.1 秒，属"相差不是很大"→ 两家都占席且份额相等
     *  （旧口径按 √总分，400ms 那家拿走约 71%，越快的越肥、赢家照样通吃）。
     *  b1 实测 5000ms：没超绝对门槛（6000ms）却掉出并列带（max(400×3, 400+3000)=3400ms）
     *  → 撤席并留 BAND_REL_SLOW；它仍在兜底顺序里，别的家挂了照样接。 */
    static void speedBuysSeatNotShare(PolicyService ps) {
        // 只留"速度"这一个维度不同：前面几组用例在 a1 上留下了失败计数与出字速率样本，
        // 不清平就是拿 H/T 的差异冒充份额差异，这个用例就白测了。
        for (String m : new String[]{"a1", "a2"}) {
            PolicyService.EntryRuntime e = ps.entry("alpha", m);
            e.consecutiveFailures = 0;
            e.qualityIssues = 0;
            e.ewmaTokensPerSec = 40;
            e.coolDownUntil = 0;
        }
        setMeasured(ps.entry("alpha", "a1"), 400);
        setMeasured(ps.entry("alpha", "a2"), 300, 1500, 1600);
        setMeasured(ps.entry("beta", "b1"), 5000);
        List<PolicyService.Candidate> three = List.of(
                new PolicyService.Candidate("alpha", "a1", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a2", "k1", 1, null),
                new PolicyService.Candidate("beta", "b1", "k1", 1, null));
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        List<PolicyService.Scored> res = ps.rank(three, "auto", false, null, null, false, null, t);
        PolicyService.RankTrace.Row a1 = row(t, "alpha", "a1");
        PolicyService.RankTrace.Row a2 = row(t, "alpha", "a2");
        PolicyService.RankTrace.Row b1 = row(t, "beta", "b1");
        check("并列带内视为同速：400ms 与 1500ms 两家都占席", a1.inBand && a2.inBand,
                "a1.inBand=" + a1.inBand + " a2.inBand=" + a2.inBand);
        check("同偏好、速度差 3.75 倍的两家份额相等（旧口径会给快的约 71%）",
                Math.abs(a1.share - a2.share) < 0.02, "a1=" + a1.share + " a2=" + a2.share);
        check("综合分仍按速度拉开（速度没被从评分里删掉，只是不再决定份额）",
                a1.score / a2.score > 1.5, "a1.score=" + a1.score + " a2.score=" + a2.score);
        check("掉出并列带但未超绝对门槛 → 撤席并留 BAND_REL_SLOW",
                !b1.inBand && PolicyService.BAND_REL_SLOW.equals(b1.bandOut),
                "inBand=" + b1.inBand + " bandOut=" + b1.bandOut);
        check("撤席说明带齐三个数（我的/最快的/门槛），界面不用猜文案",
                b1.note != null && b1.note.contains("5000") && b1.note.contains("3400"), "note=" + b1.note);
        check("留痕自带并列带参照系（沙盘要能说清门槛画在哪）",
                t.tieSlowMs == 3400 && t.fastestMs == 400, "tie=" + t.tieSlowMs + " fastest=" + t.fastestMs);
        check("撤席≠剔除：慢家仍在兜底顺序里", res.stream().anyMatch(s
                -> "beta".equals(s.candidate.providerId())), "res=" + res.size());
    }

    /** 给条目盖一个"本进程实测出来的最近首字"（连环形窗、每条样本的时刻和采样戳一起给：
     *  只改中位数、不给这些，会被当成跨重启的旧值重新回填，测的就不是这条路了）。
     *  单值调用在这里补成三条（中位数不变）：判定要窗内 ≥3 条才认——窗里只有 1~2 条时
     *  一条尖峰就占掉一半证据，speedState 判 STALE 而不是实测，fixture 若只给一两条，
     *  测的其实是"证据不足"那条路，不是"实测快/慢"。 */
    static void setMeasured(PolicyService.EntryRuntime er, long... ttftsMs) {
        if (ttftsMs.length == 1) ttftsMs = new long[]{ttftsMs[0], ttftsMs[0], ttftsMs[0]};
        er.recentTtftWin = new long[Math.max(8, ttftsMs.length)];
        er.recentTtftTs = new long[er.recentTtftWin.length];
        long now = System.currentTimeMillis();
        for (int i = 0; i < ttftsMs.length; i++) {
            er.recentTtftWin[i] = ttftsMs[i];
            er.recentTtftTs[i] = now;      // 逐样本记时刻：判定按保鲜期逐条过滤， fixture 要的是"全都刚测的"
        }
        er.recentTtftIdx = ttftsMs.length % er.recentTtftWin.length;
        er.recentTtftCount = ttftsMs.length;
        long[] s = ttftsMs.clone();
        java.util.Arrays.sort(s);
        er.recentTtftMedian = s.length % 2 == 1 ? s[s.length / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2.0;
        er.lastSpeedSampleTs = now;
        er.ttftSeeded = true;
    }

    /** 按 provider×model 取留痕行（取不到直接失败，绝不"取第一条"回退掩盖错位） */
    static PolicyService.RankTrace.Row row(PolicyService.RankTrace t, String provider, String model) {
        for (PolicyService.RankTrace.Row r : t.rows)
            if (provider.equals(r.provider) && model.equals(r.model)) return r;
        throw new AssertionError("留痕里没有 " + provider + "/" + model + " 这一行");
    }

    static double factorOf(PolicyService.RankTrace.Row r, String key) {
        Double v = r.factors == null ? null : r.factors.get(key);
        if (v == null) throw new AssertionError("留痕里没有因子 " + key);
        return v;
    }

    static boolean near(double a, double b) {
        return Math.abs(a - b) < 1e-9;
    }

    /** 席内按供应商合计份额（封顶是在"家"这一层判的，用例必须用同一个口径检验） */
    static Map<String, Double> shareByProvider(PolicyService.RankTrace t) {
        Map<String, Double> m = new LinkedHashMap<>();
        for (PolicyService.RankTrace.Row r : t.rows) {
            if (r.inBand) m.merge(r.provider, r.share, Double::sum);
        }
        return m;
    }

    /** ⑧ 出字速度也不再决定份额：同优先级、同档位、只有吞吐差 8 倍的两家，应得份额必须相等。
     *  旧口径把 S（首字）除回偏好分，却没除 T（吞吐）——分数里按吞吐罚过一遍、份额再罚一遍，
     *  "速度相差不是很大就该分摊"只兑现了一半：160 tok/s 的家会拿走 ≈2/3 流量。 */
    static void throughputIsDividedOutOfShare(PolicyService ps) {
        for (String m : new String[]{"a1", "a2"}) {
            PolicyService.EntryRuntime e = ps.entry("alpha", m);
            e.consecutiveFailures = 0;
            e.qualityIssues = 0;
            e.coolDownUntil = 0;
            setMeasured(e, 400);                                  // 首字刻意相同：只留吞吐一个维度差
            e.ewmaTokensPerSec = "a1".equals(m) ? 20 : 160;
        }
        List<PolicyService.Candidate> two = List.of(
                new PolicyService.Candidate("alpha", "a1", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a2", "k1", 1, null));
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        ps.rank(two, "auto", false, null, null, false, null, t);
        PolicyService.RankTrace.Row a1 = row(t, "alpha", "a1");
        PolicyService.RankTrace.Row a2 = row(t, "alpha", "a2");
        double t1 = factorOf(a1, "T_throughput");
        double t2 = factorOf(a2, "T_throughput");
        check("吞吐因子按实测速率拉开（20 与 160 tok/s 不是同一个 T）", t2 / t1 > 3.0, "T=" + t1 + "/" + t2);
        check("综合分仍按吞吐拉开（T 没被从评分里删掉，只是不再决定份额）",
                a2.score / a1.score > 3.0, "score=" + a1.score + "/" + a2.score);
        check("出字速度差 8 倍的两家份额相等（份额口径已除掉 T）",
                near(a1.share, a2.share), "share=" + a1.share + "/" + a2.share);
    }

    /** ⑨a 供应商份额封顶的算术（纯函数，不碰服务）：钉线 → 余量按权重分给未达线的家 → 重复到没有家超线。
     *  要钉死的五条：不触线时原样、超线者恰好等于线、回灌后总和仍是 100%、
     *  一家占两席时"按合计封顶、席内按权重分"、上限低于每家均分线时取均分（再低不可能既封顶又分完）。 */
    static void cappedSharesMath() {
        double[] off = PolicyService.providerCappedShares(new double[]{3, 1}, new String[]{"p", "q"}, 0);
        check("capPct=0＝不限，只做归一", near(off[0], 0.75) && near(off[1], 0.25), Arrays.toString(off));

        double[] single = PolicyService.providerCappedShares(new double[]{3, 1}, new String[]{"p", "p"}, 0.35);
        check("席内只有一家时封顶无意义（它必须接住 100%）",
                near(single[0], 0.75) && near(single[1], 0.25), Arrays.toString(single));

        double[] noTouch = PolicyService.providerCappedShares(new double[]{1, 1, 1}, new String[]{"p", "q", "r"}, 0.6);
        check("没有家超线时份额不动（0.333 都低于 0.6）",
                near(noTouch[0], 1 / 3.0) && near(noTouch[1], 1 / 3.0) && near(noTouch[2], 1 / 3.0),
                Arrays.toString(noTouch));

        double[] bind = PolicyService.providerCappedShares(new double[]{6, 3, 1}, new String[]{"x", "y", "z"}, 0.35);
        check("超线者逐个钉在封顶线上（x 钉住后回灌把 y 顶过线，再钉 y，余 0.30 给最弱的 z）",
                near(bind[0], 0.35) && near(bind[1], 0.35) && near(bind[2], 0.30), Arrays.toString(bind));
        double sum = 0;
        for (double v : bind) sum += v;
        check("回灌后总和仍是 100%（封顶不能凭空造出或吃掉流量）", near(sum, 1.0), "sum=" + sum);

        double[] twoSeats = PolicyService.providerCappedShares(new double[]{6, 2, 1}, new String[]{"x", "x", "y"}, 0.35);
        check("一家占两席按合计封顶、席内再按权重分（两家均分线 0.5 高于 0.35，故取 0.5）",
                near(twoSeats[0], 0.375) && near(twoSeats[1], 0.125) && near(twoSeats[2], 0.5),
                Arrays.toString(twoSeats));

        double[] floorLine = PolicyService.providerCappedShares(new double[]{9, 1, 1, 1}, new String[]{"p", "q", "r", "s"}, 0.1);
        check("上限（0.1）低于每家均分线（0.25）时取均分线",
                near(floorLine[0], 0.25) && near(floorLine[1], 0.25) && near(floorLine[2], 0.25)
                        && near(floorLine[3], 0.25), Arrays.toString(floorLine));

        double[] zeros = PolicyService.providerCappedShares(new double[]{0, 0}, new String[]{"p", "q"}, 0.35);
        check("偏好分全被压到 0 时均分（抽签仍能铺开，不是 NaN）",
                near(zeros[0], 0.5) && near(zeros[1], 0.5), Arrays.toString(zeros));
    }

    /** ⑨b 吞吐因子的取值表：评分与份额两处共用同一个纯函数，界面逐因子展示才可能连乘回总分 */
    static void throughputFactorValues() {
        check("无样本：免费 0.9 / 非免费 0.5（没历史不占便宜）",
                near(PolicyService.throughputFactor("free", -1), 0.9)
                        && near(PolicyService.throughputFactor("premium", -1), 0.5));
        check("0 tok/s 落到地板 0.4", near(PolicyService.throughputFactor("free", 0), 0.4));
        check("20 tok/s=0.65、80 tok/s=1.4（每 80 tok/s 加 1.0）",
                near(PolicyService.throughputFactor("free", 20), 0.65)
                        && near(PolicyService.throughputFactor("free", 80), 1.4));
        check("168 tok/s 起封顶 2.5（倍率不无限放大）",
                near(PolicyService.throughputFactor("free", 168), 2.5)
                        && near(PolicyService.throughputFactor("free", 5000), 2.5));
    }

    /** ⑨c 两条封顶走通整条路由：改配置 → reload → 评分 → 撤席原因 → 份额。
     *  甲（席位）：一家最多 2 席，第 3 个模型不占抽签席但仍在兜底顺序里；
     *  乙（份额）：一家合计不超过 35%，超出的按权重回灌给别家。
     *  配置走真实 YAML 重载（不是直接改内存字段），这样"界面能不能配"这件事一并被钉住。 */
    static void providerSeatCapAndShareCap(ConfigService cs, Path cfgFile, PolicyService ps) throws Exception {
        List<PolicyService.Candidate> five = List.of(
                new PolicyService.Candidate("alpha", "a1", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a2", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a9", "k1", 1, null),   // 同家第 3 个模型
                new PolicyService.Candidate("beta", "b1", "k1", 1, null),
                new PolicyService.Candidate("gamma", "g1", "k1", 1, null));
        for (PolicyService.Candidate c : five) {
            PolicyService.EntryRuntime e = ps.entry(c.providerId(), c.model());
            e.consecutiveFailures = 0;
            e.qualityIssues = 0;
            e.coolDownUntil = 0;
            e.ewmaTokensPerSec = 40;
            setMeasured(e, 400);   // 首字全部相同：这一条测的是封顶，不是速度并列带
        }
        String capped = TEST_YAML
                .replace("shareBandSize: 3", "shareBandSize: 4")
                .replace("shareProviderSeatCap: 0", "shareProviderSeatCap: 2")
                .replace("shareProviderCapPct: 0", "shareProviderCapPct: 0.35");
        Files.writeString(cfgFile, capped);
        cs.reload();

        PolicyService.RankTrace t = new PolicyService.RankTrace();
        List<PolicyService.Scored> res = ps.rank(five, "auto", false, null, null, false, null, t);
        check("留痕自带两条封顶参数（界面要说清这次按什么线画的）",
                t.providerSeatCap == 2 && near(t.providerCapPct, 0.35),
                "seatCap=" + t.providerSeatCap + " capPct=" + t.providerCapPct);

        List<PolicyService.RankTrace.Row> band = new ArrayList<>();
        List<PolicyService.RankTrace.Row> seatOut = new ArrayList<>();
        for (PolicyService.RankTrace.Row r : t.rows) {
            if (r.inBand) band.add(r);
            if (PolicyService.BAND_PROVIDER_SEATS.equals(r.bandOut)) seatOut.add(r);
        }
        long alphaSeats = band.stream().filter(r -> "alpha".equals(r.provider)).count();
        check("甲·席位封顶：alpha 三个高分模型只占 2 席", alphaSeats == 2, "alphaSeats=" + alphaSeats);
        check("甲·第 3 个模型被撤席并留 BAND_PROVIDER_SEATS", seatOut.size() == 1
                && "alpha".equals(seatOut.get(0).provider), "out=" + seatOut.size());
        check("甲·撤席说明带齐数字（已有几席、上限几席），界面不用猜文案",
                seatOut.size() == 1 && seatOut.get(0).note != null
                        && seatOut.get(0).note.contains("每家最多 2 席"), "note=" +
                        (seatOut.isEmpty() ? "" : seatOut.get(0).note));
        check("甲·撤席≠剔除：被撤的模型仍在兜底顺序里",
                res.stream().anyMatch(s -> "alpha".equals(s.candidate.providerId())
                        && "a9".equals(s.candidate.model())), "res=" + res.size());
        check("席位数仍受 bandSize 约束（4 席：alpha 2 + beta 1 + gamma 1）",
                band.size() == 4, "band=" + band.size());

        Map<String, Double> agg = shareByProvider(t);
        double tot = 0;
        for (double v : agg.values()) tot += v;
        check("乙·份额总和仍是 100%", near(tot, 1.0), "sum=" + tot);
        boolean over = agg.values().stream().anyMatch(v -> v > 0.35 + 1e-9);
        check("乙·没有一家的合计超过 35%（" + agg + "）", !over);
        check("乙·封顶线真的钉住了：合计最高的一家恰好等于 35%",
                near(agg.values().stream().mapToDouble(Double::doubleValue).max().orElse(0), 0.35),
                "max=" + agg);
        check("乙·一家占两席时两席份额相等（同家内部按权重分，不偏向某一把 Key）",
                near(row(t, "alpha", "a1").share, row(t, "alpha", "a2").share),
                "a1=" + row(t, "alpha", "a1").share + " a2=" + row(t, "alpha", "a2").share);

        // 对照：同一批候选、同样 4 席，只把两条封顶关掉——份额会重新向高分家集中。
        // 留着它是为了证明"上面那几个数是封顶带来的，不是排序巧合"。
        Files.writeString(cfgFile, TEST_YAML.replace("shareBandSize: 3", "shareBandSize: 4"));
        cs.reload();
        PolicyService.RankTrace plain = new PolicyService.RankTrace();
        ps.rank(five, "auto", false, null, null, false, null, plain);
        Map<String, Double> plainAgg = shareByProvider(plain);
        check("对照：关掉两条封顶后 alpha 一家吃掉过半（封顶确实在起作用）",
                plainAgg.getOrDefault("alpha", 0.0) > 0.5, "alpha=" + plainAgg.get("alpha"));
        check("对照：关掉席位封顶后 alpha 占满 3 席",
                plain.rows.stream().filter(r -> r.inBand && "alpha".equals(r.provider)).count() == 3);

        Files.writeString(cfgFile, TEST_YAML);
        cs.reload();
    }

    /** ⑨ 跨重启恢复的旧中位数不许"自我免疫"。
     *  旧实现只要 recentTtftMedian ≥ 0 就跳过日志回填：上周一次 20s 抽风被存进快照，重启后它挡住
     *  所有新证据，模型永远停在撤席线外（没席位→没流量→没新样本→永远"慢"）。
     *  现在只有"保鲜期（speedSampleMaxAgeHours，默认 72 小时）内的实测"才算新鲜，过期的用 request_log 重算顶掉。
     *  顺带钉三条回填口径：模型名大小写不敏感（上游目录里就是 DeepSeek-M 这种写法），修复前流式路径记下的
     *  3ms 伪样本和巡检流量都不许进速度历史，以及重算后盖的是"样本自己的时刻"而不是"本次计算的时刻"
     *  （盖计算时刻＝快照每 60 秒重写一次就把旧值洗白成最新实测，2026-10-14 的实锤）。 */
    static void staleRestoredMedianIsReplacedByLog(Database db, ConfigService cs) throws Exception {
        StatsDao dao = new StatsDao(db);
        long now = System.currentTimeMillis();
        dao.insertRequestLog(now - 40_000, "DeepSeek-M", "gamma", "DeepSeek-M",
                300, 900, 100L, 20L, "OK", 1, "auto", null, "claude-code", null, "T-seed-1", 200, null, "k1");
        dao.insertRequestLog(now - 30_000, "DeepSeek-M", "gamma", "deepseek-m",
                400, 900, 100L, 20L, "OK", 1, "auto", null, "claude-code", null, "T-seed-2", 200, null, "k1");
        dao.insertRequestLog(now - 20_000, "DeepSeek-M", "gamma", "DeepSeek-M",
                500, 900, 100L, 20L, "OK", 1, "auto", null, "claude-code", null, "T-seed-3", 200, null, "k1");
        dao.insertRequestLog(now - 15_000, "DeepSeek-M", "gamma", "DeepSeek-M",
                3, 900, 100L, 20L, "OK", 1, "auto", null, "claude-code", null, "T-seed-4", 200, null, "k1");
        dao.insertRequestLog(now - 10_000, "DeepSeek-M", "gamma", "DeepSeek-M",
                5000, 900, 100L, 20L, "OK", 1, "auto", null, "probe", null, "T-seed-5", 200, null, "k1");

        List<StatsDao.TtftSample> hist = await(() -> dao.recentOkTtfts("gamma", "deepseek-m", 8, 10_000, 50, 0),
                l -> l.size() >= 3);
        check("回填查库大小写不敏感（配置里的小写名也拿得到上游大写名的历史）",
                hist.size() == 3, "hist=" + hist);
        check("测量伪值（3ms）与巡检样本都不进速度历史",
                hist.stream().noneMatch(s -> s.ttftMs() < 50 || s.ttftMs() >= 5000), "hist=" + hist);

        PolicyService ps2 = new PolicyService(cs, dao, null);
        PolicyService.EntryRuntime er = ps2.entry("gamma", "DeepSeek-M");
        er.recentTtftMedian = 20_000;                    // 上周进快照的"抽风中位数"
        er.lastSpeedSampleTs = now - 100 * 3600_000L;     // 一百小时前的样本，早已过保鲜期
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        ps2.rank(List.of(new PolicyService.Candidate("gamma", "DeepSeek-M", "k1", 3, null),
                new PolicyService.Candidate("alpha", "a1", "k1", 1, null)),
                "auto", false, null, null, false, null, t);
        check("过期中位数被日志重算取代（300/400/500 → 400，不是 20000）",
                er.recentTtftMedian == 400, "median=" + er.recentTtftMedian);
        check("重算后盖的是最近一枚样本自己的时刻（now-20s），不是『本次计算』的一刻",
                Math.abs(er.lastSpeedSampleTs - (now - 20_000)) < 5_000, "ts=" + er.lastSpeedSampleTs
                        + " 期望≈" + (now - 20_000));
        check("速度证据恢复后重新拿到抽签席（旧中位数再也无法永久逐出快模型）",
                row(t, "gamma", "DeepSeek-M").inBand, "bandOut=" + row(t, "gamma", "DeepSeek-M").bandOut);
    }

    /** ⑩ 运行时快照的两个键 bug（"界面看不出来但一直在坏"）：
     *  ① channel_id 里的命名空间标记逐次累积——写快照用了带 \u0001 的键片段，restore 时再补一个标记，
     *     每重启一次多一个 \u0001（实测 858 行脏快照），Key 级冷却跨重启永远匹配不到；
     *  ② 同一模型按大小写分裂成两条健康记录，速度与失败历史被切成两半。
     *  钉三件事：键本身归一、写出去的快照干净、启动清理能收拾历史脏行。 */
    static void snapshotKeysStayClean(Database db, ConfigService cs, PolicyService ps) {
        StatsDao dao = new StatsDao(db);
        checkEq("entryKey 大小写归一（DeepSeek-M 与 deepseek-m 同一个键）",
                PolicyService.entryKey("alpha", "deepseek-m"), PolicyService.entryKey("alpha", "DeepSeek-M"));
        check("同一模型的不同写法共用一个运行态对象",
                ps.entry("alpha", "DeepSeek-M") == ps.entry("alpha", "deepseek-m"), "");
        checkEq("rawChannelId 剥掉命名空间标记", "k1",
                PolicyService.rawChannelId("" + (char) 1 + "k1"));
        checkEq("rawChannelId 对正常渠道号不动", "k1", PolicyService.rawChannelId("k1"));
        check("标记只剥一层，多余的留在里层（旧脏数据不会又被认成渠道号）",
                PolicyService.rawChannelId("" + (char) 1 + (char) 1 + "k1")
                        .equals("" + (char) 1 + "k1"));

        // 写出去的快照必须干净：Key 行带标记 → restore 时再加一层，每重启一次多一个，
        // 冷却恢复从此永远匹配不到（实测攒出 858 行脏快照）。
        PolicyService ps3 = new PolicyService(cs, dao, null);
        ps3.channel("alpha", "k1").coolDownUntil = System.currentTimeMillis() + 60_000;
        ps3.entry("alpha", "DeepSeek-M").consecutiveFailures = 2;
        ps3.persistSnapshot();
        List<StatsDao.SnapshotRow> written = dao.loadRuntimeSnapshots();
        check("快照里没有 \u0001 命名空间标记（Key 冷却能跨重启对上真渠道号）",
                written.stream().allMatch(r -> !r.channelId().contains(String.valueOf((char) 1))),
                "rows=" + written);
        check("模型行按小写归一存档（不再与上游大写写法各存一条历史）",
                written.stream().filter(r -> "*".equals(r.channelId()))
                        .allMatch(r -> r.model().equals(r.model().toLowerCase(Locale.ROOT))),
                "rows=" + written);

        // 历史遗留脏行（改造前跑出来的）必须被启动清理收拾掉，且不能误伤干净行
        int cleanBefore = written.size();
        dao.saveRuntimeSnapshot(List.of(
                new StatsDao.SnapshotRow("alpha", "" + (char) 1 + (char) 1 + "k1", "*",
                        null, "COOLING", 1L, 0, null, null),
                new StatsDao.SnapshotRow("alpha", "" + (char) 1 + "k2", "*",
                        null, "COOLING", 1L, 0, null, null),
                new StatsDao.SnapshotRow("beta", "*", "DeepSeek-V4-Flash",
                        null, "CLOSED", 0L, 0, null, null)));
        int cleaned = dao.cleanupRuntimeSnapshotJunk();
        check("脏快照（标记累积 + 大小写分裂旧行）被清掉", cleaned == 3, "cleaned=" + cleaned);
        check("干净行一条没动", dao.loadRuntimeSnapshots().size() == cleanBefore,
                "now=" + dao.loadRuntimeSnapshots().size() + " clean=" + cleanBefore);
    }

    static void deleteDir(Path dir) {
        try {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignore) {}
            });
        } catch (Exception ignore) {}
    }
}
