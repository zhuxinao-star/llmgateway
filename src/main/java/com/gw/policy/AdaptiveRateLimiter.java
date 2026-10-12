package com.gw.policy;

import com.gw.config.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 自适应限流学习器（动态版）：从历史调用中"学到"每家/每个模型的限流阈值，并在路由前主动避让。
 * 阈值不是一成不变——供应商的限流规则会随时段、市场、运营需要而变化，因此上限是活的：
 *
 * 1. 双向收敛：
 *    - 向下（limit）：每次触发 429，把当时窗口用量采样为更严的经验上限（只收紧不放松）。
 *    - 向上（relax）：若某家长时间没有 429 且流量在增长，说明上限可能被放宽（或当前是低峰时段），
 *      有节制地向上试探——逐步抬高经验上限，让"被放宽的额度"恢复可用，而不是永远卡在旧值。
 * 2. 时段感知：按「高峰 / 低峰」两个时段分别记录上限（白天高峰 vs 凌晨低峰真实限流很可能不同），
 *    选择当前时段对应的上限来评估压力。
 * 3. 时间衰减/淡出：某个 provider 长时间完全没流量，其经验上限按时间衰减回"未知"，
 *    避免因一次远古限流永远压缩一家（市场变了它早该恢复）。
 * 4. 手动重置：`reset(providerId)` 清空一家所有学习结果，强制重新从零学习（管理页"重新学习"按钮）。
 *
 * 避让：score() 乘 pressure 因子（≈1 - 当前窗口用量/当前时段上限），接近上限就低分 →
 * 路由主动绕开快达上限的家，转向还有余量的家（预测性避让，不等 429 才冷却）。
 *
 * 5. 撞墙密度（除瞬时压力外的第二只眼）：瞬时窗口只回答"现在这一分钟用了多少额度"，
 *    回答不了"这家是不是反复在撞 429"。免费档把额度压到 2 次/分后，路由一旦把它派到别家，
 *    窗口用量归零、压力归零、因子回 1.0，它又被当作"有余量"抽中，中签即撞墙、白耗一次重试。
 *    因此另记每渠道最近 5 分钟的 429 时刻：密度高 → 因子叠一层地板（最多 ×0.35，随样本滑出自然衰减），
 *    且连续撞墙 + 学到的额度很紧时不占分摊抽签席（仍在兜底顺序里，别的家挂了照样接）。
 */
@Service
public class AdaptiveRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(AdaptiveRateLimiter.class);

    /** 可注入时间源（测试用），默认走真实时钟 */
    public interface TimeSource { long nowMs(); }

    private volatile TimeSource time = System::currentTimeMillis;

    /** 测试/特殊用途：替换时间源（传回真实时钟） */
    public void setTime(TimeSource t) { this.time = t == null ? System::currentTimeMillis : t; }

    /** 学习窗口：按时间分桶统计窗口内请求次数 + token 数（60s） */
    private static final long WINDOW_MS = 60_000;
    private static final int BUCKETS = 60;

    /** 向上试探的参数：连续多久没 429 且流量够多 → 每次放松一个比例 */
    private static final long RELAX_SINCE_LIMIT_MS = 20 * 60_000L;   // 距上次 429 超 20 分钟才考虑放松
    private static final long RELAX_COOLDOWN_MS = 5 * 60_000L;        // 两次放松至少隔 5 分钟（防连发内连环放宽）
    private static final double RELAX_STEP = 0.25;                    // 每次放松 25%（向上试探幅度）
    private static final int RELAX_NEED_TRAFFIC = 8;                 // 需要近窗口流量 ≥8 才可信地放松
    private static final int RELAX_MIN_TRAFFIC = 3;
    /** 完全无流量多久后上限按时间淡出回未知 */
    private static final long STALE_AFTER_MS = 6 * 3600_000L;        // 6 小时无流量视为 stale
    /** 高峰时段定义（小时，0-23）：8-23 点视为高峰，其余低峰 */
    private static final int PEAK_START_HOUR = 8;
    private static final int PEAK_END_HOUR = 23;

    // ---- 撞墙密度（"一直在撞限流"这件事，光看瞬时窗口用量看不出来）----
    // 瞬时压力 = 当前 60s 窗口用量 / 学到的上限。免费档常见的失效形态是：上限学到 2 次/分，
    // 但路由把流量派到别家去了，窗口用量 0 → 压力 0 → 因子回到 1.0 → 它又被当成"有余量"抽中，
    // 中签后立刻 429、重试、再降权……如此循环，一半请求白耗在这一家的 429 上。
    // 密度统计的是"最近 5 分钟真的撞了几次墙"，样本随窗口自然滑出即衰减，不需要额外解除动作。
    public static final long LIMIT_DENSITY_WINDOW_MS = 5 * 60_000L;
    /** 窗口内撞墙次数达到此值即视为"持续撞墙"（密度=1.0，再多不再更狠） */
    private static final int DENSITY_FULL_LIMITS = 4;
    /** 密度的最大压幅：瞬时窗口有空余但一直在撞墙时，L 最低到 ×(1-0.65)=0.35 */
    private static final double DENSITY_MAX_PENALTY = 0.65;
    /** 瞬时压力的最大压幅（沿用历史值，行为不变） */
    private static final double PRESSURE_MAX_PENALTY = 0.8;
    /** 限流因子地板：再紧也不把分数压成 0——别的家全挂时它仍必须能接住（不空手而归铁律） */
    private static final double FACTOR_FLOOR = 0.2;
    /** 抽签门槛：窗口内撞墙次数下限 */
    public static final int BAND_GATE_MIN_LIMITS = 3;
    /** 抽签门槛：学到的请求上限 ≤ 此值才算"额度确实很紧"（额度宽的家撞几次只是偶发，不该撤席） */
    public static final int TIGHT_CEILING_REQ_PER_MIN = 5;
    /** 每渠道保留的撞墙时刻样本数（够覆盖"5 分钟内 4 次"的判断即可） */
    private static final int LIMIT_SAMPLE_RING = 8;

    private final ConfigService configService;
    /** 限流学习窗口按「provider + channel(账号)」粒度分桶：providerId+"\n"+channelId → ProviderState。
     *  多 key 场景下每个 key 是独立账号、独立额度，单 key 耗尽只降权那一把 key，不连坐整家。 */
    private final ConcurrentHashMap<String, ProviderState> providerStates = new ConcurrentHashMap<>();
    /** 输出 token 安全预算：providerId+"\n"+model → 学到的单请求最大安全输出 token（截断事件采样）。
     *  输出预算本质是"模型能力"，与账号无关，保持 provider×model 维度不动。 */
    private final ConcurrentHashMap<String, Integer> maxOutputTokens = new ConcurrentHashMap<>();

    public AdaptiveRateLimiter(ConfigService configService) {
        this.configService = configService;
    }

    /** 限流窗口 key：channel 为空时退化为 provider 级（兼容无 channel 的入口） */
    private static String windowKey(String providerId, String channelId) {
        return channelId == null || channelId.isBlank() ? providerId : providerId + "\n" + channelId;
    }

    /** 记录一次向某 provider×channel(账号) 转发的用量（成功/失败都算：请求本身消耗配额） */
    public void observe(String providerId, String model, String channelId, long tokensIn) {
        if (providerId == null) return;
        String key = windowKey(providerId, channelId);
        ProviderState st = providerStates.computeIfAbsent(key, k -> new ProviderState());
        long now = time.nowMs();
        st.record(now, tokensIn);
        maybeRelax(key, st, now);
    }

    /** 429 触发：以当前窗口用量作为该 provider×channel(账号) 的经验上限（向下收敛） */
    public void recordRateLimit(String providerId, String model, String channelId, long tokensIn) {
        String key = windowKey(providerId, channelId);
        ProviderState st = providerStates.computeIfAbsent(key, k -> new ProviderState());
        long now = time.nowMs();
        Observe cur = st.snapshot(now);
        // 关键：429 触发时刻的用量 —— 真实上限一定 ≤ 此刻用量（触发了说明已达顶）
        long reqCeiling = Math.max(1, cur.requests + 1);
        long tokCeiling = Math.max(1000, cur.tokensIn + tokensIn + 500);
        // 向下收敛（当前时段）——只收紧
        Ceilings c = st.ceilingFor(now);
        if (c.reqCeiling == 0 || reqCeiling < c.reqCeiling) c.reqCeiling = reqCeiling;
        if (c.tokCeiling == 0 || tokCeiling < c.tokCeiling) c.tokCeiling = tokCeiling;
        st.confidence++;
        st.lastLimitAt = now;
        st.lastTrafficAt = now;
        st.noteLimit(now);
        log.info("自适应限流学习(收紧): provider={} channel={} 时段={} 经验上限 请求≈{}/分 token≈{}/分 置信度{} 近{}分钟撞墙{}次",
                providerId, channelId, st.periodOf(now), c.reqCeiling, c.tokCeiling, st.confidence,
                LIMIT_DENSITY_WINDOW_MS / 60_000, st.limitsWithin(now, LIMIT_DENSITY_WINDOW_MS));
    }

    /** 手动重置一家：清空该 provider 下所有 channel 的学习结果（管理页"重新学习"按家操作为准），强制重新学习 */
    public boolean reset(String providerId) {
        boolean removed = providerStates.keySet().removeIf(k -> k.equals(providerId) || k.startsWith(providerId + "\n"));
        // 同时清空该 provider 的输出 token 预算学习
        maxOutputTokens.keySet().removeIf(k -> k.startsWith(providerId + "\n"));
        // 慢性层（读库的长期 429 比例）一起清：否则管理页点了"重新学习"，
        // 60 秒后它又被同一份库数据撤席，手动能力等于没有。刷新周期 60s，接受其回潮。
        if (chronicStats.containsKey(providerId)) {
            Map<String, long[]> cs = new java.util.HashMap<>(chronicStats);
            cs.remove(providerId);
            chronicStats = Map.copyOf(cs);
        }
        return removed;
    }

    /** 移除单个 (provider, model) 的输出预算学习样本（删除单个模型时调用，不影响同 provider 其它模型） */
    public void removeOutputBudget(String providerId, String model) {
        if (providerId == null || model == null) return;
        maxOutputTokens.remove(providerId + "\n" + model);
        maxOutputTokens.remove(providerId + "\n*");
    }

    /** 记录一次输出截断：本次实际输出 completionTokens 作为该家单请求输出上限的采样。
     *  只收敛不断增（向上试探交给 relax 之外单独控制——输出上限相对稳定，保守取 min）。
     *  防毒化：只有截断点严格小于用户请求的 max_tokens（或用户未设预算）才是模型真实
     *  输出上限的样本；输出恰好==请求预算说明是用户自己的小预算掐的，学进去会把该家
     *  后续所有请求钳死在这个小值上。 */
    public void recordOutputTruncation(String providerId, String model, int completionTokens, int requestedMaxTokens) {
        if (providerId == null || completionTokens <= 0) return;
        if (requestedMaxTokens > 0 && completionTokens >= requestedMaxTokens) return;
        String key = providerId + "\n" + (model == null ? "*" : model);
        Integer cur = maxOutputTokens.get(key);
        if (cur == null || completionTokens < cur) {
            maxOutputTokens.put(key, completionTokens);
            log.info("自适应输出预算: {}/{} 学到单请求最大输出≈{} token（截断事件）", providerId, model, completionTokens);
        }
    }

    /** 该 provider×model 学到的单请求最大安全输出 token；0=未学过（不钳制） */
    public int safeMaxOutputTokens(String providerId, String model) {
        Integer m = maxOutputTokens.get(providerId + "\n" + (model == null ? "*" : model));
        if (m == null && model != null) m = maxOutputTokens.get(providerId + "\n*");
        return m == null ? 0 : m;
    }

    /**
     * 当前 provider×channel(账号) 的限流压力 ∈ [0,1]：0=无压力，1=已达/超过当前时段经验上限（应避让）。
     * 无经验上限（从未 429 或已淡出）返回 0。channel 为空时退化为 provider 级。
     */
    public double ratePressure(String providerId, String model, String channelId) {
        ProviderState st = providerStates.get(windowKey(providerId, channelId));
        if (st == null) return 0.0;
        long now = time.nowMs();
        Ceilings c = st.ceilingFor(now);
        if (c.reqCeiling == 0 && c.tokCeiling == 0) return 0.0;
        Observe cur = st.snapshot(now);
        double pReq = c.reqCeiling > 0 ? Math.min(1.0, (double) cur.requests / c.reqCeiling) : 0.0;
        double pTok = c.tokCeiling > 0 ? Math.min(1.0, (double) cur.tokensIn / c.tokCeiling) : 0.0;
        return Math.max(pReq, pTok);
    }

    /** 是否应把该 provider 视为"已到限流边界"（压力接近上限且有过学习）——强制换家。
     * 当前无调用方（保留作语义兜底）；channel 粒度下按 provider 级退化评估。 */
    public boolean shouldPress(String providerId) {
        return ratePressure(providerId, null, null) >= 0.9;
    }

    /** 某渠道（provider×Key）近 {@value #LIMIT_DENSITY_WINDOW_MS} 毫秒撞 429 的次数 */
    public int channelRateLimits(String providerId, String channelId) {
        ProviderState st = providerStates.get(windowKey(providerId, channelId));
        return st == null ? 0 : st.limitsWithin(time.nowMs(), LIMIT_DENSITY_WINDOW_MS);
    }

    /** 某家（跨它名下所有 Key）近窗口撞 429 的总次数——"这一家在不在反复撞墙"的整体信号 */
    public int providerRateLimits(String providerId) {
        if (providerId == null || providerId.isBlank()) return 0;
        long now = time.nowMs();
        int n = 0;
        for (Map.Entry<String, ProviderState> e : providerStates.entrySet()) {
            String k = e.getKey();
            if (k.equals(providerId) || k.startsWith(providerId + "\n"))
                n += e.getValue().limitsWithin(now, LIMIT_DENSITY_WINDOW_MS);
        }
        return n;
    }

    /** 某家的撞墙密度 ∈ [0,1]：窗口内次数 / {@value #DENSITY_FULL_LIMITS} 次封顶 */
    public double limitDensity(String providerId) {
        return densityOf(providerRateLimits(providerId));
    }

    /** 某渠道的撞墙密度 ∈ [0,1]（降权因子按渠道用，免得一把 Key 撞墙连坐同家其它 Key） */
    public double channelLimitDensity(String providerId, String channelId) {
        return densityOf(channelRateLimits(providerId, channelId));
    }

    private static double densityOf(int limitsInWindow) {
        return Math.min(1.0, limitsInWindow / (double) DENSITY_FULL_LIMITS);
    }

    /**
     * 该 provider 学到的最紧请求上限（跨其所有 Key 取最小的非零值）；0=还没学到。
     * 走 ceilingFor() 是为了继承"6 小时无流量即淡出"——淡出后不再拿旧上限去撤席位。
     */
    public int tightestReqCeiling(String providerId) {
        if (providerId == null || providerId.isBlank()) return 0;
        long now = time.nowMs();
        long best = 0;
        for (Map.Entry<String, ProviderState> e : providerStates.entrySet()) {
            String k = e.getKey();
            if (!k.equals(providerId) && !k.startsWith(providerId + "\n")) continue;
            long c = e.getValue().ceilingFor(now).reqCeiling;
            if (c > 0 && (best == 0 || c < best)) best = c;
        }
        return (int) Math.min(Integer.MAX_VALUE, best);
    }

    /**
     * 限流因子（纯函数，便于单测）：瞬时窗口压力与近窗口撞墙密度各压一层，取更狠的那个。
     *  - 瞬时压力满 → ×0.2；密度满 → ×0.35；地板 ×0.2（绝不压成 0，别的家全挂时它仍要能接住）。
     *  - 两者都为 0 → ×1.0，与改造前完全一致（没撞过墙的家不受影响）。
     */
    public static double pressureFactor(double windowPressure, double limitDensity) {
        double byWindow = 1.0 - PRESSURE_MAX_PENALTY * clamp01(windowPressure);
        double byDensity = 1.0 - DENSITY_MAX_PENALTY * clamp01(limitDensity);
        return Math.max(FACTOR_FLOOR, Math.min(byWindow, byDensity));
    }

    /**
     * 抽签门槛（纯函数，便于单测）：近窗口持续撞墙、且学到的额度本来就紧 → 这一家的模型不占分摊抽签席。
     * 只撤席、不剔除：它仍留在严格分数顺序的兜底链里，别家挂了照样接得住。
     * 要求"额度本来就紧"是为了不误伤——宽额度家偶发几次 429 只是抖动，撤席会把流量推给更差的家。
     */
    public static boolean seatBlockedByRateLimit(int limitsInWindow, int learnedReqPerMin) {
        return limitsInWindow >= BAND_GATE_MIN_LIMITS
                && learnedReqPerMin > 0 && learnedReqPerMin <= TIGHT_CEILING_REQ_PER_MIN;
    }

    // ---- 慢性限流（第三只眼：长窗口读库，重启不丢）----
    // 瞬时压力看 60 秒、撞墙密度看 5 分钟，两个都是"爆发检测器"，而且全在内存里。
    // 实测坑：某免费上游三天 106 发里 79 发 429，但当天两次 429 相隔 6 分钟 → limits5m=0、压力=0、
    // L=1.0、抽签席照占，还凭高分拿走四成流量。"这家长期撞墙"是 request_log 里的事实，
    // 由这一层说话：内存学习重启清零，库里比例启动后第一发就能用。
    private volatile Map<String, long[]> chronicStats = Map.of();   // providerId → {真实样本数, 其中 429 数}
    private volatile long chronicStatsAt;

    /** 定期刷入（由 PolicyService 随快照调度调）：providerId → [真实流量样本数, 其中限流数]。
     *  传 null/空 = 该层关闭（不做任何降权与撤席）。 */
    public void setChronicStats(Map<String, long[]> stats, long atMs) {
        this.chronicStats = stats == null || stats.isEmpty() ? Map.of() : Map.copyOf(stats);
        this.chronicStatsAt = atMs;
    }

    /** 该家长窗口的 429 占比 ∈ [0,1]；无样本=0（未知不罚） */
    public double chronicLimitRatio(String providerId) {
        long[] v = chronicStats.get(providerId);
        if (v == null || v[0] <= 0) return 0.0;
        return Math.min(1.0, (double) v[1] / v[0]);
    }

    /** 该家长窗口的真实流量样本数（巡检/体检/确认探测都不计入） */
    public long chronicSamples(String providerId) {
        long[] v = chronicStats.get(providerId);
        return v == null ? 0 : v[0];
    }

    /** 慢性降权系数（叠在 L 因子上取更狠的一侧），阈值口径同撤席。 */
    public double chronicPenalty(String providerId, int minSamples, double seatBlockRatio) {
        return chronicPenaltyFactor(chronicLimitRatio(providerId), chronicSamples(providerId),
                minSamples, seatBlockRatio);
    }

    /**
     * 慢性降权（纯函数，便于单测）：以"撤席线"作为满罚刻度线性给。
     *  比例达撤席线 → 地板 ×0.2；一半 → ×0.6；样本不足 minSamples 或没撞过墙 → ×1.0（不罚未知）。
     *  地板与瞬时层同：长期撞墙的家仍可能在"别的家全挂"时被选中，不能把分压成 0。
     */
    public static double chronicPenaltyFactor(double limitRatio, long samples, int minSamples, double seatBlockRatio) {
        if (samples < Math.max(1, minSamples) || limitRatio <= 0 || seatBlockRatio <= 0) return 1.0;
        double rel = Math.min(1.0, limitRatio / seatBlockRatio);
        return Math.max(FACTOR_FLOOR, 1.0 - (1.0 - FACTOR_FLOOR) * rel);
    }

    /** 慢性撤席（纯函数，便于单测）：样本足够且 429 占比达到撤席线 → 不占分摊抽签席（仍可兜底）。 */
    public static boolean seatBlockedByChronic(double limitRatio, long samples, double seatBlockRatio, int minSamples) {
        return samples >= Math.max(1, minSamples) && seatBlockRatio > 0 && limitRatio >= seatBlockRatio;
    }

    private static double clamp01(double v) {
        return v < 0 ? 0 : Math.min(1.0, v);
    }

    /**
     * 向上试探（市场/时段变化导致上限放宽）：若距上次 429 已很久、且最近流量足够多而没触发 429，
     * 说明当前时段实际能扛更大的量——每次放宽一个比例。这让"被放宽的额度"重新可用，
     * 而不是永远卡在一个旧上限（双向收敛的"向上"半环）。
     */
    private void maybeRelax(String providerId, ProviderState st, long now) {
        if (st.confidence == 0) return;                       // 从未学过的家无需试探
        if (now - st.lastLimitAt < RELAX_SINCE_LIMIT_MS) return;  // 距上次 429 太近，保守不动
        if (st.lastRelaxAt > 0 && now - st.lastRelaxAt < RELAX_COOLDOWN_MS) return; // 刚放松过，等冷却
        Ceilings c = st.ceilingFor(now);
        if (st.confidence == 0) return;                       // 可能刚被时间淡出清零，不再试探
        if (c.reqCeiling == 0 && c.tokCeiling == 0) return;
        Observe cur = st.snapshot(now);
        // 只有"当前窗口实际用量 ≥ 一定量"且仍未触发 429，才是可信的放松信号
        if (cur.requests < RELAX_MIN_TRAFFIC) return;
        if (cur.requests < RELAX_NEED_TRAFFIC) return;
        // 放宽：向上试探一个比例（不会一次性放到没边）
        long newReq = c.reqCeiling > 0 ? (long) Math.ceil(c.reqCeiling * (1 + RELAX_STEP)) : 0;
        long newTok = c.tokCeiling > 0 ? (long) Math.ceil(c.tokCeiling * (1 + RELAX_STEP)) : 0;
        boolean changed = false;
        if (newReq > c.reqCeiling && c.reqCeiling > 0) { c.reqCeiling = newReq; changed = true; }
        if (newTok > c.tokCeiling && c.tokCeiling > 0) { c.tokCeiling = newTok; changed = true; }
        if (changed) {
            st.lastRelaxAt = now;
            log.info("自适应限流学习(放松): provider={} 时段={} 试探放宽 请求上限→{}/分 token→{}/分 (up {}%)",
                    providerId, st.periodOf(now), c.reqCeiling, c.tokCeiling, (int) (RELAX_STEP * 100));
        }
    }

    /** 学习到的规则概览（管理页诊断） */
    public Map<String, Object> view() {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> providers = new LinkedHashMap<>();
        long now = time.nowMs();
        for (Map.Entry<String, ProviderState> e : providerStates.entrySet()) {
            ProviderState st = e.getValue();
            Ceilings peak = st.peak;
            Ceilings low = st.low;
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("peak", ceilingView(peak));
            p.put("low", ceilingView(low));
            p.put("confidence", st.confidence);
            p.put("lastLimitAt", st.lastLimitAt);
            p.put("lastRelaxAt", st.lastRelaxAt);
            int recent = st.limitsWithin(now, LIMIT_DENSITY_WINDOW_MS);
            p.put("limitsRecent", recent);
            p.put("limitDensity", Math.round(densityOf(recent) * 1000.0) / 1000.0);
            p.put("current", Map.of(
                    "reqPerMin", st.snapshot(now).requests,
                    "tokPerMin", st.snapshot(now).tokensIn));
            providers.put(e.getKey(), p);
        }
        out.put("windowMs", WINDOW_MS);
        out.put("peakHours", PEAK_START_HOUR + "-" + PEAK_END_HOUR);
        out.put("relaxStepPct", (int) (RELAX_STEP * 100));
        out.put("limitDensityWindowMs", LIMIT_DENSITY_WINDOW_MS);
        out.put("densityFullLimits", DENSITY_FULL_LIMITS);
        out.put("bandGate", Map.of("minLimits", BAND_GATE_MIN_LIMITS,
                "tightReqPerMin", TIGHT_CEILING_REQ_PER_MIN));
        out.put("chronic", chronicView());
        out.put("maxOutputTokens", new LinkedHashMap<>(maxOutputTokens));
        out.put("providers", providers);
        return out;
    }

    /** 慢性层概览（诊断："这家凭什么被压到地板/撤了席"要能给看数据的理由；只读库，重启也在） */
    private Map<String, Object> chronicView() {
        Map<String, Object> rows = new LinkedHashMap<>();
        for (Map.Entry<String, long[]> e : chronicStats.entrySet()) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("samples", e.getValue()[0]);
            r.put("rateLimits", e.getValue()[1]);
            r.put("limitRatio", Math.round(chronicLimitRatio(e.getKey()) * 1000.0) / 1000.0);
            rows.put(e.getKey(), r);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("updatedAt", chronicStatsAt);
        out.put("providers", rows);
        return out;
    }

    private Map<String, Object> ceilingView(Ceilings c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("learnedReqPerMin", c.reqCeiling);
        m.put("learnedTokPerMin", c.tokCeiling);
        return m;
    }

    /** 单个 provider 的窗口状态（1 秒粒度环形桶 + 分时段的经验上限 + 放松状态 + 撞墙时刻样本） */
    private final class ProviderState {
        private final long[] reqBuckets = new long[BUCKETS];
        private final long[] tokBuckets = new long[BUCKETS];
        private final long[] bucketSecs = new long[BUCKETS];
        /** 最近 LIMIT_SAMPLE_RING 次 429 的时刻（毫秒），环形覆盖最旧样本 */
        private final long[] limitTimes = new long[LIMIT_SAMPLE_RING];
        private int limitHead = 0;

        final Ceilings peak = new Ceilings();   // 高峰时段上限
        final Ceilings low = new Ceilings();    // 低峰时段上限
        int confidence = 0;
        long lastLimitAt = 0;
        long lastRelaxAt = 0;
        long lastTrafficAt = 0;

        ProviderState() {
        }

        /** 记一次撞墙时刻 */
        synchronized void noteLimit(long now) {
            limitTimes[limitHead] = now;
            limitHead = (limitHead + 1) % limitTimes.length;
        }

        /** 落在 [now-windowMs, now] 内的撞墙次数。样本会随窗口滑出，所以"停止撞墙"本身就能自动复原。 */
        synchronized int limitsWithin(long now, long windowMs) {
            long cut = now - windowMs;
            int n = 0;
            for (long t : limitTimes) if (t >= cut && t <= now) n++;
            return n;
        }

        synchronized void record(long now, long tokensIn) {
            long sec = now / 1000L;
            int idx = (int) ((sec % BUCKETS + BUCKETS) % BUCKETS);
            if (bucketSecs[idx] != sec) {
                bucketSecs[idx] = sec;
                reqBuckets[idx] = 0;
                tokBuckets[idx] = 0;
            }
            reqBuckets[idx] = reqBuckets[idx] + 1;
            tokBuckets[idx] = tokBuckets[idx] + Math.max(0, tokensIn);
            lastTrafficAt = now;
        }

        synchronized Observe snapshot(long now) {
            long sec = now / 1000L;
            long cut = sec - WINDOW_MS / 1000L;
            long req = 0, tok = 0;
            for (int i = 0; i < BUCKETS; i++) {
                if (bucketSecs[i] > cut && bucketSecs[i] <= sec) {
                    req += reqBuckets[i];
                    tok += tokBuckets[i];
                }
            }
            return new Observe(req, tok);
        }

        /** 当前时刻属于高峰还是低峰 */
        String periodOf(long now) {
            return isPeak(now) ? "peak" : "low";
        }

        /** 取当前时段对应的经验上限（含时间淡出：太久没流量视为"须重新学习"） */
        Ceilings ceilingFor(long now) {
            // 时间淡出：6 小时无流量 → 视为未知（市场早变了），强制重新学
            if (lastTrafficAt > 0 && now - lastTrafficAt > STALE_AFTER_MS) {
                peak.reqCeiling = 0; peak.tokCeiling = 0;
                low.reqCeiling = 0; low.tokCeiling = 0;
                confidence = 0;
            }
            return isPeak(now) ? peak : low;
        }
    }

    private static boolean isPeak(long now) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(now);
        int h = cal.get(Calendar.HOUR_OF_DAY);
        return h >= PEAK_START_HOUR && h < PEAK_END_HOUR;
    }

    /** 一家的经验上限（一个时段） */
    private static class Ceilings {
        long reqCeiling = 0;   // 每分钟请求数上限
        long tokCeiling = 0;   // 每分钟 token 数上限
    }

    private record Observe(long requests, long tokensIn) {}
}