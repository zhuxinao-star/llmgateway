package com.gw.policy;

import com.gw.config.ConfigService;
import com.gw.store.Database;
import com.gw.store.StatsDao;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * 速度样本保鲜期 + 三态「没有有效实测」+ 体检值只判定 + 探索测量席（main 直跑）。
 *
 * 2026-10-14 排查 agnes 零流量时实锤的那条路：它上一次真实流量是 09-28 13:51，同秒被压测打出
 * 11.6~16.9s 的排队首字，8 格里 6 格是那一批 → 中位数 12727.5ms 十七天一动不动；当天并列带门槛
 * 3864ms 判它 BAND_SLOW，于是没席位→没流量→没新样本→中位数永远不被推翻。上一轮的席位下限
 * 救不了它：下限管"带内别缩成一两家"，而 agnes 是从一开始就被"十七天前的旧值"当成慢家挡在带外——
 * 这不是抽签不公平，是测量失效。
 *
 * 钉六件事：
 *  ① 新鲜度逐样本看「这一条自己发生的时刻」（recentTtftTs），不看整窗共用一个戳、更不看"什么时候算的"：
 *    快照每 60 秒重写一次，拿计算时刻冒充采样时刻＝旧值永生；一条新样本盖全窗旧值＝鬼值当新鲜；
 *  ② 三态（Null 不是值）：MEASURED 才参与判慢，STALE（有测量但窗内凑不出中位数：全过期／来源时刻追不到／
 *    条数不够）与 UNKNOWN（从没测过）都读成"无有效实测"走中性档——但界面必须能把两者分开，处置动作完全不同；
 *  ③ 判定读的是"保鲜期窗内样本的中位数"，不是"环窗（按条数）中位数"：旧尖峰自己出窗，
 *    不许继续把窗内好样本的中位数拖高（agnes 2026-10-14 锁死的那条路）；
 *  ④ 回填带时间界：比保鲜期还老的日志样本不再占用"最近 8 条"这个坑；
 *  ⑤ 体检（client=probe）值只走"拦"这一条路（否决探索席），绝不进分数、并列带参照系与份额；
 *  ⑥ 探索测量席在带宽之外另开座（否则带宽一满就永远补不上，这条通道等于不存在），
 *    但同家席位封顶等硬门槛一律不让路；关掉（0）必须完整退回旧行为。
 * 用临时 SQLite + 临时 YAML，不碰真实 config.yaml 与 data/ 库。
 */
public class SpeedSampleFreshnessTest {

    static int failures = 0;

    static void check(String name, boolean cond) {
        check(name, cond, "");
    }

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    static final long DAY = 86_400_000L;
    static final long MAX_AGE = 72 * 3600_000L;    // 与 TEST_YAML 的 speedSampleMaxAgeHours: 72 对齐

    /** 缩小版当天现场：两家新鲜快模型占满带宽，其余三家分别是"过期值/从没测过/只有体检值" */
    static final String TEST_YAML = """
            strategy: auto
            gateway-keys:
              - test-key
            defaults:
              loadShare: true
              shareBandSize: 2
              shareSlowTtftMs: 6000
              shareSpeedTieRatio: 3
              shareProviderSeatCap: 1
              shareProviderCapPct: 0
              shareMinSeats: 0
              shareExploreSeats: 2
              speedSampleMaxAgeHours: 72
            providers:
              - id: alpha
                name: alpha
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [a_fast, a_never]
                channels:
                  - id: k1
                    api-key: x
              - id: beta
                name: beta
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [b_fast]
                channels:
                  - id: k1
                    api-key: x
              - id: gamma
                name: gamma
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [g_never]
                channels:
                  - id: k1
                    api-key: x
              - id: delta
                name: delta
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [d_stale]
                channels:
                  - id: k1
                    api-key: x
              - id: zeta
                name: zeta
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [z_probe]
                channels:
                  - id: k1
                    api-key: x
              - id: eta
                name: eta
                base-url: http://127.0.0.1:9/v1
                priority: 1
                default-cost-tier: free
                static-models: [e_log, p_log, e_one]
                channels:
                  - id: k1
                    api-key: x
            """;

    static List<PolicyService.Candidate> bandFixture() {
        return List.of(
                new PolicyService.Candidate("alpha", "a_fast", "k1", 1, null),
                new PolicyService.Candidate("beta", "b_fast", "k1", 1, null),
                new PolicyService.Candidate("alpha", "a_never", "k1", 1, null),
                new PolicyService.Candidate("gamma", "g_never", "k1", 1, null),
                new PolicyService.Candidate("delta", "d_stale", "k1", 1, null),
                new PolicyService.Candidate("zeta", "z_probe", "k1", 1, null));
    }

    public static void main(String[] args) throws Exception {
        triStateMath();
        freshReadAndProbeVeto();
        exploreOrderMath();
        sampleAgeTextMath();

        Path dir = Files.createTempDirectory("gw-freshness");
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

            staleFieldStaysInBand(cs, ps);
            probeVetoesExploreSeat(cs, ps);
            exploreSeatOffIsExactOldBehaviour(cs, ps);
            backfillRespectsFreshnessWindow(db, cs, dao, ps);
            traceCarriesThreeStates(cs, ps);
        } finally {
            deleteDir(dir);
        }
        System.out.println(failures == 0 ? "\nALL PASS" : "\nFAILURES=" + failures);
        if (failures > 0) System.exit(1);
    }

    // ---------------- ① 三态判据（纯函数） ----------------

    static void triStateMath() {
        PolicyService.EntryRuntime none = new PolicyService.EntryRuntime();
        check("没有任何速度值＝UNKNOWN（从没测过，界面该说『没测过』而不是『慢』）",
                PolicyService.speedState(none, MAX_AGE, System.currentTimeMillis()) == PolicyService.SpeedState.UNKNOWN);

        PolicyService.EntryRuntime fresh = entry(800, System.currentTimeMillis() - 3600_000L);
        check("保鲜期内（1 小时前的样本 < 72 小时）＝MEASURED",
                PolicyService.speedState(fresh, MAX_AGE, System.currentTimeMillis()) == PolicyService.SpeedState.MEASURED);

        long now = System.currentTimeMillis();
        PolicyService.EntryRuntime old = entry(12727, now - 17 * DAY);
        check("十七天前的中位数＝STALE（agnes 的现场：旧值还挂着，但它已经不是『最新实测』）",
                PolicyService.speedState(old, MAX_AGE, now) == PolicyService.SpeedState.STALE);

        PolicyService.EntryRuntime noStamp = entry(12727, 0);
        check("有值但来源时刻追不到＝STALE，不是 MEASURED（快照恢复值不许自称新鲜）",
                PolicyService.speedState(noStamp, MAX_AGE, now) == PolicyService.SpeedState.STALE);
        check("来源时刻未知的过期值，界面拿不到精确年龄（不许编一个『N 小时前』）",
                noStamp.lastSpeedSampleTs == 0);

        check("保鲜期设 0＝用户显式关闭过期判定，旧值照旧算数（退回改造前行为，能力不没收）",
                PolicyService.speedState(old, 0, now) == PolicyService.SpeedState.MEASURED);

        PolicyService.EntryRuntime artifact = new PolicyService.EntryRuntime();
        artifact.recentTtftMedian = 3;                    // 修复前流式路径记下的计量伪值
        artifact.lastSpeedSampleTs = now;
        check("低于地板的伪样本连值都不算（0 速不是快，是 bug）",
                PolicyService.speedState(artifact, MAX_AGE, now) == PolicyService.SpeedState.UNKNOWN);

        // agnes 2026-10-15 的现场：快照带回来的 12727.5 老中位 + 保鲜期内只回填到 1 条真实样本
        PolicyService.EntryRuntime one = ghostWithOneSample(12727, now - 3600_000L);
        check("窗里只有 1 条样本时，中位数不是它算出来的→不许自称新鲜实测（STALE）",
                PolicyService.speedState(one, MAX_AGE, now) == PolicyService.SpeedState.STALE);
        check("只有 1 条样本＋保鲜期判定关闭：用户显式要旧值算数，能力不没收",
                PolicyService.speedState(one, 0, now) == PolicyService.SpeedState.MEASURED);

        // agnes 2026-10-16 的现场（修完鬼值之后露出的下一层）：窗内 2 条，一条 448、一条昨天的 11771。
        // 2 条的"中位数"是两数均值＝尖峰占掉一半证据，6109ms 照样把一家钉出并列带。
        PolicyService.EntryRuntime two = new PolicyService.EntryRuntime();
        window(two, new long[]{11771, 448}, new long[]{now - 18 * 3600_000L, now - 3600_000L});
        check("窗内只有 2 条（整窗都新鲜）：尖峰占一半证据，凑不出判定用的中位数→STALE",
                PolicyService.speedState(two, MAX_AGE, now) == PolicyService.SpeedState.STALE,
                "median=" + two.recentTtftMedian);
        check("它读成『无有效实测』而不是 6109ms（旧口径正是拿这个均值判慢的）",
                PolicyService.freshTtft(two, MAX_AGE, now) < 0,
                "fresh=" + PolicyService.freshTtft(two, MAX_AGE, now));

        // 时间尺逐样本落地：旧尖峰自己出窗，不许再把窗内好样本的中位数拖高
        PolicyService.EntryRuntime mixed = new PolicyService.EntryRuntime();
        window(mixed, new long[]{15000, 16000, 17000, 400, 500, 600},
                new long[]{now - 100 * DAY, now - 100 * DAY, now - 100 * DAY,
                        now - 3600_000L, now - 3600_000L, now - 3600_000L});
        check("判定读的是保鲜期窗内的中位数（500ms），不是整窗中位数（7800ms 里全是三个月前的排队）",
                PolicyService.freshTtft(mixed, MAX_AGE, now) == 500,
                "fresh=" + PolicyService.freshTtft(mixed, MAX_AGE, now)
                        + " whole=" + mixed.recentTtftMedian);
        check("整窗展示值照旧保留（历史账不删，只是不再拿它判今天）",
                mixed.recentTtftMedian == 7800, "whole=" + mixed.recentTtftMedian);
        check("窗内样本够 3 条＝MEASURED，且证据条数报得出来",
                PolicyService.speedState(mixed, MAX_AGE, now) == PolicyService.SpeedState.MEASURED
                        && PolicyService.freshTtftSamples(mixed, MAX_AGE, now) == 3,
                "n=" + PolicyService.freshTtftSamples(mixed, MAX_AGE, now));
        check("关闭过期判定时条数按整窗算（退回旧口径，别伪造一个『窗内 0 条』）",
                PolicyService.freshTtftSamples(mixed, 0, now) == 6,
                "n=" + PolicyService.freshTtftSamples(mixed, 0, now));
    }

    static void freshReadAndProbeVeto() {
        long now = System.currentTimeMillis();
        check("判慢只读新鲜实测：MEASURED 给值",
                PolicyService.freshTtft(entry(800, now - DAY), MAX_AGE, now) == 800);
        check("过期值读成 -1（＝无有效实测），绝不拿它判慢",
                PolicyService.freshTtft(entry(12727, now - 17 * DAY), MAX_AGE, now) < 0);
        check("没测过同样是 -1，两条路在判慢上必须同档",
                PolicyService.freshTtft(new PolicyService.EntryRuntime(), MAX_AGE, now) < 0);
        check("1 条样本撑不起的快照旧中位同样读成 -1：不判慢，才有资格占探索席去补第 2 条",
                PolicyService.freshTtft(ghostWithOneSample(12727, now - 3600_000L), MAX_AGE, now) < 0);

        check("无体检样本不否决：未知不是慢的证据",
                !PolicyService.probeVetoesExplore(-1, 0, MAX_AGE, 6000, now));
        check("体检慢过绝对门槛才否决",
                PolicyService.probeVetoesExplore(9000, now - 3600_000L, MAX_AGE, 6000, now));
        check("体检不慢就不拦（照样给它探索席去拿真实样本）",
                !PolicyService.probeVetoesExplore(1500, now - 3600_000L, MAX_AGE, 6000, now));
        check("体检值自己也过期了不否决：不能拿两个月前的体检结论省掉今天的一次测量",
                !PolicyService.probeVetoesExplore(9000, now - 100 * DAY, MAX_AGE, 6000, now));
        check("体检来源时刻未知（0）不否决：来源不明的数字没有拦人的资格",
                !PolicyService.probeVetoesExplore(9000, 0, MAX_AGE, 6000, now));
        check("关闭过期判定时，体检值长期有效（与 speedState 同一把尺）",
                PolicyService.probeVetoesExplore(9000, now - 100 * DAY, 0, 6000, now));
    }

    // ---------------- ② 探索席补席算术（纯函数） ----------------

    static void exploreOrderMath() {
        long now = System.currentTimeMillis();
        List<PolicyService.ExploreCand> pool = List.of(
                new PolicyService.ExploreCand("delta", "d_stale", false, now - 17 * DAY),
                new PolicyService.ExploreCand("gamma", "g_never", true, 0),
                new PolicyService.ExploreCand("zeta", "z_old", false, now - 200 * DAY),
                new PolicyService.ExploreCand("theta", "t_unknown", false, 0));
        List<String> seated = List.of("alpha", "beta");

        List<Integer> one = PolicyService.exploreSeatOrder(pool, seated, 2, 0, 1);
        check("从没测过的排最前（未知最该补一次测量，而不是按过期时刻排队）",
                one.equals(List.of(1)), "picked=" + one);
        check("带宽（2 席）已满仍能补：探索席是在带宽之外另开座，否则这条通道等于不存在",
                one.size() == 1);

        List<Integer> two = PolicyService.exploreSeatOrder(pool, seated, 2, 0, 2);
        check("只补 exploreSeats 席就停（池里四家候选也只补两家，用户说的探索成本就是这张帽）",
                two.size() == 2, "picked=" + two);

        List<Integer> all = PolicyService.exploreSeatOrder(pool, seated, 2, 0, 4);
        check("过期值之间按样本时刻从旧到新，来源时刻未知（0）视为最旧：连哪天测的都说不清",
                all.equals(List.of(1, 3, 2, 0)), "order=" + all);

        check("exploreSeats=0 是显式关闭，一条也不补",
                PolicyService.exploreSeatOrder(pool, seated, 2, 0, 0).isEmpty());
        check("探索池空时无事可做",
                PolicyService.exploreSeatOrder(List.of(), seated, 2, 0, 3).isEmpty());

        List<Integer> drained = PolicyService.exploreSeatOrder(pool, seated, 2, 0, 5);
        check("意向数大于池子时按池子收口（不再有别的隐藏上限把这条通道掐死），但总席位不超 带宽＋意向",
                drained.size() == pool.size() && seated.size() + drained.size() <= 2 + 5,
                "picked=" + drained);

        List<PolicyService.ExploreCand> sameHouse = List.of(
                new PolicyService.ExploreCand("alpha", "a_never", true, 0),
                new PolicyService.ExploreCand("gamma", "g_never", true, 0));
        List<Integer> skipFull = PolicyService.exploreSeatOrder(sameHouse, seated, 4, 1, 2);
        check("同家席位封顶探索席也不越过（alpha 已有 1 席，只能补 gamma）",
                skipFull.equals(List.of(1)), "picked=" + skipFull);

        List<PolicyService.ExploreCand> tie = List.of(
                new PolicyService.ExploreCand("zzz", "z1", true, 0),
                new PolicyService.ExploreCand("aaa", "a1", true, 0));
        check("同档按家名＋模型定序（同一输入可重放，单测才敢断言次序）",
                PolicyService.exploreSeatOrder(tie, List.of("p"), 4, 0, 2).equals(List.of(1, 0)));
    }

    static void sampleAgeTextMath() {
        long now = System.currentTimeMillis();
        check("来源时刻未知就直说不知道", PolicyService.sampleAgeText(0, now).equals("来源时刻未知"));
        check("整月按天说", PolicyService.sampleAgeText(now - 17 * DAY, now).equals("17 天"));
        check("不到一天按小时", PolicyService.sampleAgeText(now - 5 * 3600_000L, now).equals("5 小时"));
        check("不到一小时按分钟（至少 1 分钟，别说 0 分钟）",
                PolicyService.sampleAgeText(now - 30_000L, now).equals("1 分钟"));
    }

    // ---------------- ③ 整条路由：过期值不判慢 + 探索席拿到样本 ----------------

    /** 现场复现：d_stale 挂着十七天前的 12727.5ms。旧实现判它 BAND_SLOW（此后再也没有新样本推翻它）；
     *  现在它读作"无有效实测"，带宽满了也在带宽之外补进探索席，并且拿到应得份额——只有真接到流量，
     *  速度估值才可能自证。 */
    static void staleFieldStaysInBand(ConfigService cs, PolicyService ps) {
        seed(ps);
        PolicyService.RankTrace t = trace();
        List<PolicyService.Scored> res = ps.rank(bandFixture(), "auto", false, null, null, false, null, t);

        PolicyService.RankTrace.Row d = row(t, "delta", "d_stale");
        check("过期慢值再也不判 BAND_SLOW（十七天前那次排队不是今天的证据）",
                !PolicyService.BAND_SLOW.equals(d.bandOut), "bandOut=" + d.bandOut);
        check("过期值读作无有效实测：中性档，不是按 12727ms 判死",
                factorOf(d, "ttftEstMs") < 0, "ttftEstMs=" + d.factors.get("ttftEstMs"));
        check("d_stale 本轮占探索测量席（补席之后才谈得上重新测量）", d.inBand, "bandOut=" + d.bandOut);
        check("留痕说清『上次实测在 17 天、已过期』（界面不许把放宽席伪装成正常入席）",
                d.note != null && d.note.contains("17 天") && d.note.contains("探索测量席"), "note=" + d.note);
        check("占席就应有份额：拿到流量才能刷新自己的速度估值", d.share > 0, "share=" + d.share);

        PolicyService.RankTrace.Row g = row(t, "gamma", "g_never");
        check("从没测过的 gamma 同样拿到探索席（与过期值是两条来路，同一个处置）", g.inBand, "bandOut=" + g.bandOut);
        check("留痕区分『从没拿到过有效实测』与『旧值过期』",
                g.note != null && g.note.contains("从没拿到过有效实测"), "note=" + g.note);

        check("留痕把三个数记全：带宽 2 席＋探索 2 席，探索席开关与保鲜期都带上",
                t.seatsStrict == 2 && t.seatsRelaxed == 0 && t.seatsExplore == 2
                        && t.exploreSeats == 2 && t.speedSampleMaxAgeHours == 72,
                "strict=" + t.seatsStrict + " relaxed=" + t.seatsRelaxed + " explore=" + t.seatsExplore
                        + " cfg=" + t.exploreSeats + "/" + t.speedSampleMaxAgeHours);
        check("主选理由里看得见探索席（取数的人不用数行）",
                t.pickReason != null && t.pickReason.contains("探索测量席"), "reason=" + t.pickReason);
        check("撤席≠剔除：六发候选全在兜底顺序里", res.size() == 6, "res=" + res.size());

        double sum = 0;
        int seats = 0;
        for (PolicyService.RankTrace.Row r : t.rows) {
            if (r.inBand) { sum += r.share; seats++; }
            else check("未占席的行份额记 0", r.share == 0, r.provider + "/" + r.model + "=" + r.share);
        }
        check("探索席只改『谁入座』，份额口径照旧（席内合计 100%）",
                seats == 4 && Math.abs(sum - 1.0) < 1e-9, "seats=" + seats + " sum=" + sum);

        // 同家席位封顶：探索席也不越过它（alpha 已有一席）
        PolicyService.RankTrace.Row an = row(t, "alpha", "a_never");
        check("同家席位封顶在收集阶段就挡下：a_never 没座（凑席位不是让门槛让路）",
                !an.inBand && PolicyService.BAND_PROVIDER_SEATS.equals(an.bandOut),
                "inBand=" + an.inBand + " bandOut=" + an.bandOut);
    }

    /** 体检值只走"拦"这一条路：巡检已经测出这一家慢过绝对门槛，就不该再拿用户的真实请求去试第二次；
     *  但它体检出来的 9000ms 一步也不进分数、并列带参照系与份额。 */
    static void probeVetoesExploreSeat(ConfigService cs, PolicyService ps) {
        seed(ps);
        PolicyService.RankTrace t = trace();
        ps.rank(bandFixture(), "auto", false, null, null, false, null, t);

        PolicyService.RankTrace.Row z = row(t, "zeta", "z_probe");
        check("只有体检证据且体检已慢过门槛 → BAND_PROBE_SLOW，不占探索席",
                !z.inBand && PolicyService.BAND_PROBE_SLOW.equals(z.bandOut),
                "inBand=" + z.inBand + " bandOut=" + z.bandOut);
        check("留痕写明省下的是用户的真实请求",
                z.note != null && z.note.contains("巡检体检") && z.note.contains("用户的真实请求"), "note=" + z.note);
        check("体检值不进结算：这一发的实测首字仍是『无有效实测』",
                factorOf(z, "ttftEstMs") < 0, "ttftEstMs=" + z.factors.get("ttftEstMs"));
        check("体检值也不替全组画门槛：并列带仍由最快那发新鲜实测定（400ms → 3400ms）",
                t.fastestMs == 400 && t.tieSlowMs == 3400, "fastest=" + t.fastestMs + " tie=" + t.tieSlowMs);

        PolicyService.EntryRuntime er = ps.entry("zeta", "z_probe");
        check("体检值只存在体检字段里（与结算用的中位数分家）",
                er.probeTtftMedianMs == 9000 && er.recentTtftMedian < 0,
                "probe=" + er.probeTtftMedianMs + " median=" + er.recentTtftMedian);
    }

    /** 界面把探索席改成 0 必须完整退回旧行为：席位只由带宽与并列带决定，一家也不多补 */
    static void exploreSeatOffIsExactOldBehaviour(ConfigService cs, PolicyService ps) throws Exception {
        seed(ps);
        cs.persist(cfg -> cfg.getDefaults().setShareExploreSeats(0));
        try {
            PolicyService.RankTrace t = trace();
            ps.rank(bandFixture(), "auto", false, null, null, false, null, t);
            check("shareExploreSeats=0：一条探索席也不补（本轮仍是带宽内那 2 席）",
                    t.seatsExplore == 0 && t.exploreSeats == 0 && banded(t).size() == 2,
                    "explore=" + t.seatsExplore + " band=" + banded(t));
            PolicyService.RankTrace.Row d = row(t, "delta", "d_stale");
            check("关掉后过期值依然不判慢：它只是排在席外，不是被当成慢家",
                    !d.inBand && !PolicyService.BAND_SLOW.equals(d.bandOut)
                            && !PolicyService.BAND_PROBE_SLOW.equals(d.bandOut),
                    "bandOut=" + d.bandOut);
        } finally {
            cs.persist(cfg -> cfg.getDefaults().setShareExploreSeats(2));
        }
    }

    // ---------------- ④ 回填：时间界 + 体检旁证 ----------------

    /** 只留条数上限不留时间界时，"最近 8 条"这个坑会被十七天前的样本永久占着——这正是 agnes 锁死的原因。
     *  钉三条：查询本身带时间界；回填结果与新鲜度用的都是样本自己的时刻；体检旁证单独取、只用于判定。 */
    static void backfillRespectsFreshnessWindow(Database db, ConfigService cs, StatsDao dao, PolicyService ps)
            throws Exception {
        long now = System.currentTimeMillis();
        // 保鲜期外的老样本（100 天前，很快）＋保鲜期内的两条（900/1000）
        log(dao, now - 100 * DAY, "eta", "e_log", 300, "claude-code", "T-old");
        log(dao, now - 2 * 3600_000L, "eta", "e_log", 900, "claude-code", "T-new-1");
        log(dao, now - 3600_000L, "eta", "e_log", 1000, "claude-code", "T-new-2");
        // 只有体检流量的家（8900/9100，client=probe）
        log(dao, now - 2 * 3600_000L, "eta", "p_log", 8900, "probe", "T-probe-1");
        log(dao, now - 3600_000L, "eta", "p_log", 9100, "probe", "T-probe-2");

        List<StatsDao.TtftSample> unbounded = await(() -> dao.recentOkTtfts("eta", "e_log", 8, 10_000, 50, 0),
                l -> l.size() >= 3);
        check("不限时刻时三条都在（对照：老样本确实在库里）", unbounded.size() == 3, "got=" + unbounded);
        List<StatsDao.TtftSample> bounded = dao.recentOkTtfts("eta", "e_log", 8, 10_000, 50, now - MAX_AGE);
        check("查询带时间界：比保鲜期还老的样本不再占用『最近 8 条』", bounded.size() == 2, "got=" + bounded);

        PolicyService.EntryRuntime er = ps.entry("eta", "e_log");
        ps.rank(List.of(new PolicyService.Candidate("eta", "e_log", "k1", 1, null)),
                "auto", false, null, null, false, null, new PolicyService.RankTrace());
        check("回填只用保鲜期内的样本（900/1000 → 950，不是把 100 天前的 300 拖进来）",
                er.recentTtftMedian == 950, "median=" + er.recentTtftMedian);
        check("新鲜度盖的是最近样本自己的时刻（不是本次计算时刻，否则每 60 秒被快照洗白）",
                Math.abs(er.lastSpeedSampleTs - (now - 3600_000L)) < 60_000, "ts=" + er.lastSpeedSampleTs);
        check("回填只捞到 2 条＝尖峰占一半证据，还凑不出判定用的中位数→先按 STALE（不判慢、继续拿量）",
                PolicyService.speedState(er, MAX_AGE, System.currentTimeMillis()) == PolicyService.SpeedState.STALE
                        && PolicyService.freshTtftSamples(er, MAX_AGE, System.currentTimeMillis()) == 2,
                "state=" + PolicyService.speedState(er, MAX_AGE, System.currentTimeMillis())
                        + " n=" + PolicyService.freshTtftSamples(er, MAX_AGE, System.currentTimeMillis()));
        // 能自愈才是这条规矩的意义：占着席→拿到第 3 发→中位数成立→从此按实测判
        ps.recordSuccess("eta", "e_log", "k1", 700L, 100L, 20L, false, "java", 900L);
        check("第 3 发新鲜样本进窗后立刻恢复 MEASURED，判定读窗内中位数 900（900/1000/700）",
                PolicyService.speedState(er, MAX_AGE, System.currentTimeMillis()) == PolicyService.SpeedState.MEASURED
                        && PolicyService.freshTtft(er, MAX_AGE, System.currentTimeMillis()) == 900,
                "fresh=" + PolicyService.freshTtft(er, MAX_AGE, System.currentTimeMillis()));
        check("逐样本记时刻：新进来这发的格子有自己的采样时刻，不是整窗共用一个戳",
                er.recentTtftTs != null && Math.abs(er.recentTtftTs[2] - System.currentTimeMillis()) < 60_000,
                "ts=" + java.util.Arrays.toString(er.recentTtftTs));

        PolicyService.EntryRuntime pr = ps.entry("eta", "p_log");
        ps.rank(List.of(new PolicyService.Candidate("eta", "p_log", "k1", 1, null)),
                "auto", false, null, null, false, null, new PolicyService.RankTrace());
        check("体检旁证单独回填（8900/9100 → 9000）", pr.probeTtftMedianMs == 9000,
                "probe=" + pr.probeTtftMedianMs);
        check("体检样本绝不进结算中位数：没有真实流量就是没有", pr.recentTtftMedian < 0,
                "median=" + pr.recentTtftMedian);
        check("体检慢过门槛 → 否决探索席；一旦有真实流量，它只作旁证",
                PolicyService.probeVetoesExplore(pr.probeTtftMedianMs, pr.probeSampleTs, MAX_AGE, 6000,
                        System.currentTimeMillis()));

        // ---- 只回填到 1 条样本：鬼中位不许被盖章成"新鲜实测"（agnes 2026-10-15 的现场）----
        log(dao, now - 70 * 3600_000L, "eta", "e_one", 11771, "java", "T-one-only");
        await(() -> dao.recentOkTtfts("eta", "e_one", 8, 10_000, 50, now - MAX_AGE), l -> l.size() >= 1);
        PolicyService.EntryRuntime one = ps.entry("eta", "e_one");
        one.recentTtftMedian = 12727.5;      // 快照恢复回来的旧中位：值在，采样时刻不在
        one.ewmaTtftMs = 8852;               // 同一份快照的 EWMA，同样不带采样时刻
        ps.rank(List.of(new PolicyService.Candidate("eta", "e_one", "k1", 1, null)),
                "auto", false, null, null, false, null, new PolicyService.RankTrace());
        check("保鲜期内只捞到 1 条→环窗只有 1 格（对照现场：agnes 近 72 小时只有 1 发真实请求）",
                one.recentTtftCount == 1, "count=" + one.recentTtftCount);
        check("快照旧中位仍然挂在展示字段上（历史账不删），但它已经没有样本为今天作证",
                one.recentTtftMedian == 12727.5 && PolicyService.freshTtftSamples(one, MAX_AGE,
                        System.currentTimeMillis()) == 1,
                "median=" + one.recentTtftMedian);
        check("1 条样本＋刚回填的时间戳＝STALE，不是 MEASURED（这一条正是把 12727.5 洗白的那道缝）",
                PolicyService.speedState(one, MAX_AGE, System.currentTimeMillis())
                        == PolicyService.SpeedState.STALE);
        check("判慢读不到它→不再每发都 BAND_REL_SLOW，而是回到『无有效实测』那条能自愈的路",
                PolicyService.freshTtft(one, MAX_AGE, System.currentTimeMillis()) < 0);
    }

    // ---------------- ⑤ 留痕往返 ----------------

    static void traceCarriesThreeStates(ConfigService cs, PolicyService ps) {
        seed(ps);
        PolicyService.RankTrace t = trace();
        t.requestedModel = "any";
        ps.rank(bandFixture(), "auto", false, null, null, false, null, t);
        check("留痕分得清『从没测过』与『旧值过期』",
                row(t, "gamma", "g_never").speedState.equals(PolicyService.SpeedState.UNKNOWN.name())
                        && row(t, "delta", "d_stale").speedState.equals(PolicyService.SpeedState.STALE.name()),
                "g=" + row(t, "gamma", "g_never").speedState + " d=" + row(t, "delta", "d_stale").speedState);
        check("新鲜实测的行不用记状态（常态不占体积，界面默认按 MEASURED 渲染）",
                row(t, "alpha", "a_fast").speedState.equals(PolicyService.SpeedState.MEASURED.name()),
                "a=" + row(t, "alpha", "a_fast").speedState);

        PolicyService.RankTrace back = PolicyService.RankTrace.fromJson(t.toJson());
        check("探索席三个数能过 JSON 往返（采样库/界面读的是同一条留痕）",
                back.seatsExplore == 2 && back.exploreSeats == 2 && back.speedSampleMaxAgeHours == 72,
                "explore=" + back.seatsExplore + "/" + back.exploreSeats + "/" + back.speedSampleMaxAgeHours);
        PolicyService.RankTrace.Row d = row(back, "delta", "d_stale");
        check("过期状态与年龄一起往返（界面要能说『样本是 17 天前的』）",
                PolicyService.SpeedState.STALE.name().equals(d.speedState)
                        && Math.abs(d.speedAgeHours - 17 * 24) < 1,
                "state=" + d.speedState + " age=" + d.speedAgeHours);
        check("窗内证据条数随状态一起往返：界面要说清是『整窗都过期』还是『窗内样本太少』",
                d.speedSamples == 0 && row(t, "delta", "d_stale").speedSamples == 0,
                "samples=" + d.speedSamples);
        check("新鲜实测的行不记条数（常态不占体积，往返后为 -1）",
                row(back, "alpha", "a_fast").speedSamples < 0,
                "samples=" + row(back, "alpha", "a_fast").speedSamples);
        check("新鲜实测的留痕不占体积：往返后状态字段为空（界面按常态渲染，只有证据不可信才记下来）",
                row(back, "alpha", "a_fast").speedState == null
                        && row(back, "alpha", "a_fast").speedAgeHours < 0,
                "state=" + row(back, "alpha", "a_fast").speedState);
    }

    // ---------------- helpers ----------------

    /** alpha/beta 新鲜快样本 + delta 十七天前的 12727.5ms + zeta 只有体检 9000ms + alpha 第三发无实测 */
    static void seed(PolicyService ps) {
        measured(ps.entry("alpha", "a_fast"), 400);
        measured(ps.entry("beta", "b_fast"), 450);
        long staleTs = System.currentTimeMillis() - 17 * DAY;
        // agnes 的现场：整窗三条样本都是 17 天前那批，值还挂在快照里，新鲜度一条都不剩
        window(ps.entry("delta", "d_stale"),
                new long[]{12727, 12727, 12728}, new long[]{staleTs, staleTs, staleTs});
        PolicyService.EntryRuntime z = ps.entry("zeta", "z_probe");
        z.probeTtftMedianMs = 9000;
        z.probeSampleTs = System.currentTimeMillis() - 3600_000L;
        z.ttftSeeded = true;
    }

    /** 盖一个"本进程刚刚实测出来的最近首字"（3 条同样本的窗：判定要 ≥3 条才认，
     *  少于 3 条时中位数没有尖峰免疫——见 PolicyService.MIN_JUDGMENT_SAMPLES）。 */
    static void measured(PolicyService.EntryRuntime er, long ttftMs) {
        window(er, new long[]{ttftMs, ttftMs, ttftMs},
                new long[]{System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis()});
        er.ttftSeeded = true;
    }

    /** 直接造一个环窗：值 + 每条样本自己的时刻（两数组一格一条，和生产路径同形状） */
    static void window(PolicyService.EntryRuntime er, long[] ttftsMs, long[] tsMs) {
        er.recentTtftWin = ttftsMs;
        er.recentTtftTs = tsMs;
        er.recentTtftIdx = 0;
        er.recentTtftCount = ttftsMs.length;
        long[] sorted = ttftsMs.clone();
        java.util.Arrays.sort(sorted);
        er.recentTtftMedian = sorted.length % 2 == 1
                ? sorted[sorted.length / 2] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0;
        long newest = 0;
        for (long t : tsMs) newest = Math.max(newest, t);
        er.lastSpeedSampleTs = newest;
    }

    /** 纯函数用例用的条目：中位数 + 它的采样时刻（窗内 3 条同值样本＝这个值确实是它们算出来的） */
    static PolicyService.EntryRuntime entry(long medianMs, long sampleTs) {
        PolicyService.EntryRuntime er = new PolicyService.EntryRuntime();
        window(er, new long[]{medianMs, medianMs, medianMs}, new long[]{sampleTs, sampleTs, sampleTs});
        return er;
    }

    /** 快照带回来的旧中位＋保鲜期内只回填到 1 条样本：agnes 2026-10-15 的现场 */
    static PolicyService.EntryRuntime ghostWithOneSample(long medianMs, long sampleTs) {
        PolicyService.EntryRuntime er = new PolicyService.EntryRuntime();
        er.recentTtftWin = new long[]{medianMs};
        er.recentTtftTs = new long[]{sampleTs};
        er.recentTtftCount = 1;
        er.lastSpeedSampleTs = sampleTs;
        er.recentTtftMedian = medianMs;   // 快照恢复值：一条样本算不出中位数，它不是这条样本的中位
        return er;
    }

    static void log(StatsDao dao, long ts, String provider, String model, int ttftMs, String client, String reqId) {
        dao.insertRequestLog(ts, model, provider, model, ttftMs, 900, 100L, 20L, "OK", 1, "auto", null,
                client, null, reqId, 200, null, "k1");
    }

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

    static double factorOf(PolicyService.RankTrace.Row r, String key) {
        Double v = r.factors == null ? null : r.factors.get(key);
        if (v == null) throw new AssertionError("留痕里没有因子 " + key + "（" + r.provider + "/" + r.model + "）");
        return v;
    }

    static <T> T await(java.util.function.Supplier<T> get, java.util.function.Predicate<T> ready)
            throws Exception {
        T v = null;
        for (int i = 0; i < 100; i++) {
            v = get.get();
            if (ready.test(v)) return v;
            Thread.sleep(50);
        }
        return v;
    }

    static void deleteDir(Path dir) {
        try {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignore) {}
            });
        } catch (Exception ignore) {}
    }
}
