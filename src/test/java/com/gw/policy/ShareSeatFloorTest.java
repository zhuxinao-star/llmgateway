package com.gw.policy;

import com.gw.config.ConfigService;
import com.gw.store.Database;
import com.gw.store.StatsDao;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * 席位下限（shareMinSeats）单测（main 直跑）：钉死"速度只排队、不缩池"这条不变量。
 *
 * 2026-10-14 实锤的坑：并列带是硬过滤——本组只要有一发 848ms 的快模型，门槛就被钉在
 * max(848×3, 848+3000)=3848ms，32 个候选/9 家里过绝对门槛的有 19 个，最后只有 2 家占席。
 * 抽签再怎么公平也只能在两家里分，用户配的其余家永远拿不到流量（"怼着一家死磕"的真凶），
 * 而且闭环自锁：被撤席→没流量→没新样本→估值停在旧慢值→继续被撤席。
 *
 * 钉六件事：
 *  ① 补席决策的算术（纯函数）：达到下限即停、无实测排最前、其余按实测首字升序、同 TTFT 按家名定序可重放；
 *  ② bandCap 与同家席位封顶是硬上限，补席不越过；
 *  ③ minSeats=0 是显式关闭，退回"速度说了算"；
 *  ④ 整条路由走通：带内 2 家 + 补 1 家 = 3 席，补的是带外最快的那家；
 *  ⑤ 硬门槛一律不让路——BAND_SLOW（绝对慢）与 BAND_PROVIDER_SEATS（同家席位封顶）挡下的，
 *    绝不因为"席位不够"被补回来（撤席只能是硬约束，不能是速度相对值之外的猜测）；
 *  ⑥ 补席只改"谁入座"，不改份额口径（席内份额仍来自偏好分，总和 100%），留痕三个数能往返 JSON。
 * 用临时 SQLite + 临时 YAML，不碰真实 config.yaml 与 data/ 库。
 */
public class ShareSeatFloorTest {

    static int failures = 0;

    static void check(String name, boolean cond) {
        check(name, cond, "");
    }

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    /** 2026-10-14 那一线场的缩小版 fixture：两家在带内、三家在带外、其中一家慢过绝对门槛 */
    static final String TEST_YAML = """
            strategy: auto
            gateway-keys:
              - test-key
            defaults:
              loadShare: true
              shareBandSize: 4
              shareSlowTtftMs: 14000
              shareSpeedTieRatio: 3
              shareProviderSeatCap: 1
              shareProviderCapPct: 0
              shareMinSeats: 3
            providers:
              - id: alpha
                name: alpha
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [a_fast, a_slow]
                channels:
                  - id: k1
                    api-key: x
              - id: beta
                name: beta
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [b_mid]
                channels:
                  - id: k1
                    api-key: x
              - id: gamma
                name: gamma
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [g_out]
                channels:
                  - id: k1
                    api-key: x
              - id: delta
                name: delta
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [d_out]
                channels:
                  - id: k1
                    api-key: x
              - id: zeta
                name: zeta
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [z_abs]
                channels:
                  - id: k1
                    api-key: x
            """;

    /** provider/model → 实测首字（ms），量级取自 2026-10-14 真实留痕 */
    static final Map<String, Long> TTFT = Map.of(
            "alpha/a_fast", 848L,     // 带内（本组最快）
            "alpha/a_tie", 1842L,     // 在带内，但 alpha 的一席已被 a_fast 占掉
            "beta/b_mid", 1842L,      // 带内（< 3848）
            "alpha/a_out", 4100L,     // 带外最快的一家——但同家封顶，补席不能越过它
            "gamma/g_out", 4407L,     // 带外（可补），最快 eligible → 第一个补进席
            "delta/d_out", 4454L,     // 带外（可补），下限更高时才补进来
            "zeta/z_abs", 16753L);    // 慢过绝对门槛 14000 → BAND_SLOW，硬门槛

    static List<PolicyService.Candidate> all() {
        return List.of(
                new PolicyService.Candidate("alpha", "a_fast", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a_tie", "k1", 1, null),
                new PolicyService.Candidate("beta", "b_mid", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a_out", "k1", 1, null),
                new PolicyService.Candidate("gamma", "g_out", "k1", 1, null),
                new PolicyService.Candidate("delta", "d_out", "k1", 1, null),
                new PolicyService.Candidate("zeta", "z_abs", "k1", 1, null));
    }

    public static void main(String[] args) throws Exception {
        relaxMath();
        Path dir = Files.createTempDirectory("gw-seatfloor");
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

            seed(ps);
            tieThresholdIsTheLiveOne(ps);
            seatsReachFloorAndPickTheFastestOutsider(ps);
            hardGatesNeverYield(ps);
            minSeatsZeroKeepsSpeedOnly(cs, ps);
            relaxStopsAtBandCap(cs, ps);
            traceNumbersRoundTripJson(ps);
        } finally {
            deleteDir(dir);
        }
        System.out.println(failures == 0 ? "\nALL PASS" : "\nFAILURES=" + failures);
        if (failures > 0) System.exit(1);
    }

    // ---------------- ① ② ③ 补席算术（纯函数，不碰服务） ----------------

    static void relaxMath() {
        List<PolicyService.SeatCand> pool = List.of(
                new PolicyService.SeatCand("slow", 9000),
                new PolicyService.SeatCand("unmeasured", -1),
                new PolicyService.SeatCand("mid", 4407),
                new PolicyService.SeatCand("fast", 4400));

        check("已够下限就不补（带内 3 席 ≥ 下限 3）",
                PolicyService.relaxSeatOrder(pool, List.of("p", "q", "r"), 4, 0, 3).isEmpty());
        check("minSeats=0＝显式关闭补席，退回速度说了算",
                PolicyService.relaxSeatOrder(pool, List.of("p"), 4, 0, 0).isEmpty());
        check("补席池空时无事可做",
                PolicyService.relaxSeatOrder(List.of(), List.of("p"), 4, 0, 3).isEmpty());

        List<Integer> order = PolicyService.relaxSeatOrder(pool, List.of("p"), 5, 0, 5);
        check("次序＝无实测最前，其余按实测首字升序（让步也要让给带外最快的家）",
                order.equals(List.of(1, 3, 2, 0)), "order=" + order);

        List<Integer> two = PolicyService.relaxSeatOrder(pool, List.of("p"), 4, 0, 3);
        check("达到下限即停（补 2 席到 3，不把带外三家全拉进来）",
                two.equals(List.of(1, 3)), "picked=" + two);

        List<Integer> capped = PolicyService.relaxSeatOrder(pool, List.of("p"), 3, 0, 5);
        check("bandCap 是硬上限：下限 5 也只补到带宽 3 席", capped.equals(List.of(1, 3)), "picked=" + capped);

        List<PolicyService.SeatCand> sameHouse = List.of(
                new PolicyService.SeatCand("alpha", 4000),
                new PolicyService.SeatCand("beta", 5000));
        List<Integer> skipFull = PolicyService.relaxSeatOrder(sameHouse, List.of("alpha"), 4, 1, 3);
        check("同家席位封顶补席也不越过（alpha 已满 1 席，只能补 beta）",
                skipFull.equals(List.of(1)), "picked=" + skipFull);

        List<PolicyService.SeatCand> tie = List.of(
                new PolicyService.SeatCand("zzz", 5000),
                new PolicyService.SeatCand("aaa", 5000));
        check("同实测按家名定序（同一输入可重放，单测才敢断言次序）",
                PolicyService.relaxSeatOrder(tie, List.of("p"), 4, 0, 3).equals(List.of(1, 0)));
    }

    // ---------------- ④ 整条路由：带内 2 家补到 3 席 ----------------

    /** 门槛算式本身也用当天那一发钉住：848ms 最快 → 并列带 3848ms（不是 2544ms） */
    static void tieThresholdIsTheLiveOne(PolicyService ps) {
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        ps.rank(all(), "auto", false, null, null, false, null, t);
        check("并列带门槛＝max(848×3, 848+3000)＝3848ms",
                t.tieSlowMs == 3848 && t.fastestMs == 848, "tie=" + t.tieSlowMs + " fastest=" + t.fastestMs);
    }

    static void seed(PolicyService ps) {
        for (Map.Entry<String, Long> e : TTFT.entrySet()) {
            String[] pm = e.getKey().split("/");
            PolicyService.EntryRuntime er = ps.entry(pm[0], pm[1]);
            er.consecutiveFailures = 0;
            er.qualityIssues = 0;
            er.coolDownUntil = 0;
            er.ewmaTokensPerSec = 40;
            measured(er, e.getValue());
        }
    }

    /** 盖一个"本进程实测出来的最近首字"（连环形窗与时间戳一起给，否则会被当成跨重启旧值重算） */
    static void measured(PolicyService.EntryRuntime er, long ttftMs) {
        er.recentTtftWin = new long[]{ttftMs, ttftMs};
        er.recentTtftIdx = 0;
        er.recentTtftCount = 2;
        er.recentTtftMedian = ttftMs;
        er.medianTs = System.currentTimeMillis();
        er.ttftSeeded = true;
    }

    static void seatsReachFloorAndPickTheFastestOutsider(PolicyService ps) {
        PolicyService.RankTrace t = trace();
        ps.rank(all(), "auto", false, null, null, false, null, t);
        List<String> band = banded(t);
        check("席位下限生效：带内只剩 2 家时补到 3 家", band.size() == 3, "band=" + band);
        check("补的是带外最快的那家（gamma 4407ms），不是分数高的",
                band.contains("gamma"), "band=" + band);
        check("留痕自带三个数（带内几席、放宽几席、下限几席）",
                t.seatFloor == 3 && t.seatsStrict == 2 && t.seatsRelaxed == 1,
                "floor=" + t.seatFloor + " strict=" + t.seatsStrict + " relaxed=" + t.seatsRelaxed);
        PolicyService.RankTrace.Row g = row(t, "gamma", "g_out");
        check("补进来的席位把撤席标记改掉了，但原因写在 note 里（界面不能说谎）",
                g.inBand && g.bandOut == null && g.note != null && g.note.contains("席位下限"),
                "bandOut=" + g.bandOut + " note=" + g.note);
        check("主选理由说清本轮放宽过（取数的人不用去数行）",
                t.pickReason != null && t.pickReason.contains("补 1 席"), "reason=" + t.pickReason);
        // 负反馈闭环：没席位＝没流量＝没新样本。补席后这一家有份额，下一轮就有真测量。
        check("补进来的家有应得份额（拿到流量才能刷新自己的速度估值）", g.share > 0, "share=" + g.share);
    }

    /** ⑤ 席位不够不是让硬门槛让路的理由——绝对慢与同家封顶挡下的，绝不因为"凑数"被补回来 */
    static void hardGatesNeverYield(PolicyService ps) {
        PolicyService.RankTrace t = trace();
        ps.rank(all(), "auto", false, null, null, false, null, t);
        List<String> band = banded(t);
        check("绝对慢（16753ms > 14000ms）绝不因为凑席位被补回来",
                !band.contains("zeta") && PolicyService.BAND_SLOW.equals(row(t, "zeta", "z_abs").bandOut),
                "band=" + band + " out=" + row(t, "zeta", "z_abs").bandOut);
        check("同家席位封顶（每家 1 席）补席也不越过：alpha 本轮只有一个模型带流量",
                band.stream().filter("alpha"::equals).count() == 1, "band=" + band);
        PolicyService.RankTrace.Row aOut = row(t, "alpha", "a_out");
        check("带外最快的 alpha/a_out（4100ms，比 gamma、delta 都快）没被补进席——它输在同家封顶",
                !aOut.inBand && PolicyService.BAND_REL_SLOW.equals(aOut.bandOut),
                "inBand=" + aOut.inBand + " bandOut=" + aOut.bandOut);
        check("在带内但同家满席的 alpha/a_tie 保持 BAND_PROVIDER_SEATS（没被放宽伪装成进席）",
                PolicyService.BAND_PROVIDER_SEATS.equals(row(t, "alpha", "a_tie").bandOut),
                "bandOut=" + row(t, "alpha", "a_tie").bandOut);
        check("delta（4454ms）本轮下限已满 3 席，不再多让",
                !band.contains("delta") && PolicyService.BAND_REL_SLOW.equals(row(t, "delta", "d_out").bandOut),
                "band=" + band);
        check("撤席≠剔除：慢家仍在兜底顺序里（不空手而归铁律）",
                ps.rank(all(), "auto", false, null, null, false, null, new PolicyService.RankTrace()).size() == 7, "");
    }

    /** ③ 界面把下限改成 0 必须真的退回旧行为 */
    static void minSeatsZeroKeepsSpeedOnly(ConfigService cs, PolicyService ps) throws Exception {
        cs.persist(cfg -> cfg.getDefaults().setShareMinSeats(0));
        PolicyService.RankTrace t = trace();
        ps.rank(all(), "auto", false, null, null, false, null, t);
        check("minSeats=0：只留带内 2 席，一条放宽也没有",
                banded(t).size() == 2 && t.seatsRelaxed == 0 && t.seatFloor == 0,
                "band=" + banded(t) + " relaxed=" + t.seatsRelaxed);
        cs.persist(cfg -> cfg.getDefaults().setShareMinSeats(3));
    }

    /** ② 下限高于带宽时按带宽收口：补到 bandCap 就停 */
    static void relaxStopsAtBandCap(ConfigService cs, PolicyService ps) throws Exception {
        cs.persist(cfg -> cfg.getDefaults().setShareMinSeats(6));
        PolicyService.RankTrace t = trace();
        ps.rank(all(), "auto", false, null, null, false, null, t);
        check("下限（6）> 带宽（4）时取带宽：本轮 4 席，带外两家按快慢补进来",
                banded(t).size() == 4 && t.seatsStrict == 2 && t.seatsRelaxed == 2,
                "band=" + banded(t) + " strict=" + t.seatsStrict + " relaxed=" + t.seatsRelaxed);
        check("补席按实测首字升序：4407 的 gamma 先于 4454 的 delta",
                row(t, "gamma", "g_out").inBand && row(t, "delta", "d_out").inBand);
        cs.persist(cfg -> cfg.getDefaults().setShareMinSeats(3));
    }

    /** ⑥ 补席只动"谁入座"，份额口径与留痕往返不能被带跑 */
    static void traceNumbersRoundTripJson(PolicyService ps) {
        PolicyService.RankTrace t = trace();
        t.requestedModel = "any";
        ps.rank(all(), "auto", false, null, null, false, null, t);
        double sum = 0;
        for (PolicyService.RankTrace.Row r : t.rows) {
            if (r.inBand) sum += r.share;
            else check("未进席的行份额记 0", r.share == 0, r.provider + "/" + r.model + "=" + r.share);
        }
        check("补进来的席位照样参与份额归一（席内合计 100%）", Math.abs(sum - 1.0) < 1e-9, "sum=" + sum);
        PolicyService.RankTrace back = PolicyService.RankTrace.fromJson(t.toJson());
        check("三个席位数能过 JSON 往返（采样库/界面读的是同一条留痕）",
                back.seatFloor == 3 && back.seatsStrict == 2 && back.seatsRelaxed == 1,
                "floor=" + back.seatFloor + " strict=" + back.seatsStrict + " relaxed=" + back.seatsRelaxed);
    }

    // ---------------- helpers ----------------

    static PolicyService.RankTrace trace() {
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        t.requestedModel = "any";
        return t;
    }

    /** 占席的 provider 列表（按留痕行顺序；同家多模型会重复出现） */
    static List<String> banded(PolicyService.RankTrace t) {
        List<String> p = new ArrayList<>();
        for (PolicyService.RankTrace.Row r : t.rows) if (r.inBand) p.add(r.provider);
        return p;
    }

    static PolicyService.RankTrace.Row row(PolicyService.RankTrace t, String provider, String model) {
        for (PolicyService.RankTrace.Row r : t.rows)
            if (provider.equals(r.provider) && model.equals(r.model)) return r;
        throw new AssertionError("留痕里没有 " + provider + "/" + model + " 这一行");
    }

    static void deleteDir(Path dir) {
        try {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignore) {}
            });
        } catch (Exception ignore) {}
    }
}
