package com.gw.policy;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.provider.adaptor.Json;
import com.gw.store.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 路由决策留痕（"评分与分摊"的数据源）。
 *
 * <p>两层级：内存环形（最近 {@value #MEM_CAPACITY} 次，界面秒开）+ 采样落库（默认 5% 抽中，
 * 重启后仍可回看历史某一次为什么给了谁）。为什么不能全量落库：一次决策快照含全部候选的
 * 因子分解，约 5~15KB，而网关每秒可能有好几发 agent 请求——全量写会把 SQLite 写队列压满，
 * 反噬转发链路（读写共用一条连接与一把锁）。
 *
 * <p>预览（评分沙盘）不进这里：它不改变运行态、也不代表真实发生过的一次调用，
 * 由端点直接返回给界面。
 */
@Service
public class RankTraceStore {

    private static final Logger log = LoggerFactory.getLogger(RankTraceStore.class);
    private static final int MEM_CAPACITY = 200;
    /** 落库行数上限：超出即从最旧开始删（与 keepDays 取交集，两道保险都要过） */
    private static final int KEEP_ROWS = 3000;
    /** 单条快照最多存多少行候选（界面要看的只是头部与出局者，尾部截掉不影响判读） */
    private static final int MAX_STORED_ROWS = 40;
    private static final int TRIM_EVERY = 100;

    private final Database db;
    private final ConfigService configService;
    private final Deque<PolicyService.RankTrace> mem = new ArrayDeque<>();
    private final AtomicInteger writesSinceTrim = new AtomicInteger();

    public RankTraceStore(Database db, ConfigService configService) {
        this.db = db;
        this.configService = configService;
    }

    /**
     * 本次真实请求是否留痕。只有关闭开关才返回 null——调用方据此决定给不给 rank() 传收集器
     * （传了就会给每个候选分配因子表）。采样只管落库，不管内存：
     * 用户要的是"每次工具调用时的候选排序"，环形里的最近 200 次必须一条不落，
     * 否则界面刚发完请求都查不到这一次；真正受采样保护的是写库（见 {@link #commit}）。
     */
    public PolicyService.RankTrace begin(String traceId, String requestedModel, String client, String endpoint) {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        if (!d.isRankTraceEnabled()) return null;
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        t.traceId = traceId;
        t.requestedModel = requestedModel;
        t.client = client;
        t.endpoint = endpoint;
        return t;
    }

    /** 决策完成后入账：内存环形恒收，写库按采样率抽中才落 */
    public void commit(PolicyService.RankTrace t) {
        if (t == null || t.preview) return;
        synchronized (mem) {
            mem.addFirst(t);
            while (mem.size() > MEM_CAPACITY) mem.removeLast();
        }
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        if (!d.isRankTraceEnabled()) return;
        double rate = d.getRankTraceSampleRate();
        if (rate <= 0) return;
        if (rate < 1.0 && ThreadLocalRandom.current().nextDouble() >= rate) return;
        persist(t, d.getRankTraceKeepDays());
        if (writesSinceTrim.incrementAndGet() >= TRIM_EVERY) {
            writesSinceTrim.set(0);
            trim(d.getRankTraceKeepDays());
        }
    }

    private void persist(PolicyService.RankTrace t, int keepDays) {
        Map<String, Object> json = t.toJson();
        Object rows = json.get("rows");
        if (rows instanceof List<?> list && list.size() > MAX_STORED_ROWS) {
            json.put("rows", new ArrayList<>(list.subList(0, MAX_STORED_ROWS)));
            json.put("rowsTruncated", list.size());
        }
        String sql = """
            INSERT INTO rank_trace(trace_id, ts, requested_model, client, endpoint, strategy,
              has_tools, picked_provider, picked_model, pick_reason, n_rows, n_filtered, payload)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        db.asyncWrite(sql, t.traceId, t.ts, t.requestedModel, t.client, t.endpoint, t.strategy,
                t.hasTools ? 1 : 0, t.pickedProvider, t.pickedModel, t.pickReason,
                t.rows.size(), t.filtered.size(), Json.write(json));
    }

    /** 保留窗口：按时间删旧的，再按行数裁掉超出上限的最旧部分 */
    private void trim(int keepDays) {
        try {
            long cutoff = System.currentTimeMillis() - Math.max(1, keepDays) * 86_400_000L;
            db.asyncWrite("DELETE FROM rank_trace WHERE ts < ?", cutoff);
            db.asyncWrite("DELETE FROM rank_trace WHERE id NOT IN "
                    + "(SELECT id FROM rank_trace ORDER BY ts DESC LIMIT ?)", KEEP_ROWS);
        } catch (Exception e) {
            log.warn("[rank-trace] 清理失败: {}", e.toString());
        }
    }

    /** 界面列表：内存里最近的决策摘要（不含 payload，点开详情再取） */
    public List<Map<String, Object>> recent(int limit, String client, String model) {
        List<PolicyService.RankTrace> snapshot;
        synchronized (mem) {
            snapshot = new ArrayList<>(mem);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (PolicyService.RankTrace t : snapshot) {
            if (out.size() >= Math.max(1, limit)) break;
            if (client != null && !client.isBlank() && !client.equalsIgnoreCase(String.valueOf(t.client))) continue;
            if (model != null && !model.isBlank() && !model.equalsIgnoreCase(String.valueOf(t.requestedModel))) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ts", t.ts);
            m.put("traceId", t.traceId);
            m.put("requestedModel", t.requestedModel);
            m.put("client", t.client);
            m.put("endpoint", t.endpoint);
            m.put("strategy", t.strategy);
            m.put("hasTools", t.hasTools);
            m.put("candidates", t.rows.size());
            m.put("filtered", t.filtered.size());
            m.put("pickedProvider", t.pickedProvider);
            m.put("pickedModel", t.pickedModel);
            m.put("pickReason", t.pickReason);
            out.add(m);
        }
        return out;
    }

    /** 某一次决策的完整快照：先查内存，落空再查采样库 */
    public PolicyService.RankTrace byTraceId(String traceId) {
        if (traceId == null || traceId.isBlank()) return null;
        synchronized (mem) {
            for (PolicyService.RankTrace t : mem) if (traceId.equals(t.traceId)) return t;
        }
        try {
            String payload = db.queryOne(
                    "SELECT payload FROM rank_trace WHERE trace_id = ? ORDER BY ts DESC LIMIT 1",
                    rs -> rs.getString(1), traceId);
            if (payload == null || payload.isBlank()) return null;
            return PolicyService.RankTrace.fromJson(Json.parseObject(payload));
        } catch (Exception e) {
            log.warn("[rank-trace] 读取 {} 失败: {}", traceId, e.toString());
            return null;
        }
    }

    /** 采样库里的时间跨度与条数（界面说明"历史回看覆盖到哪天"） */
    public Map<String, Object> storedInfo() {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            Long n = db.queryOne("SELECT COUNT(*) FROM rank_trace", rs -> rs.getLong(1));
            Long oldest = db.queryOne("SELECT MIN(ts) FROM rank_trace", rs -> rs.getLong(1));
            Long newest = db.queryOne("SELECT MAX(ts) FROM rank_trace", rs -> rs.getLong(1));
            out.put("stored", n == null ? 0 : n);
            out.put("oldestTs", oldest);
            out.put("newestTs", newest);
        } catch (Exception e) {
            out.put("stored", 0);
            out.put("error", String.valueOf(e.getMessage()));
        }
        out.put("memCapacity", MEM_CAPACITY);
        out.put("keepRows", KEEP_ROWS);
        return out;
    }

    /**
     * 实际调用分布（分摊对照的"实际"一侧）：按 provider×model 聚合窗口内请求数。
     *
     * <p>口径：跨所有来源工具汇总（agent 流量 + 网关内部压缩调用都算，它们都真实消耗上游配额）；
     * {@code client} 非空时只看该来源。历史实现带了 {@code AND client = ''}，那是 usage_daily
     * 加 client 维度迁移时给老数据回填的空串——于是把真正来自 agent 的调用全滤掉了，
     * 界面只显示出实际量的三成，"没分摊下去"的结论被严重低估。
     */
    public List<Map<String, Object>> actualUsage(String fromDay, String toDay, String client) {
        boolean byClient = client != null && !client.isBlank();
        String sql = "SELECT provider_id, model, SUM(req_count), SUM(tokens_in+tokens_out) FROM usage_daily"
                + " WHERE day BETWEEN ? AND ?"
                + (byClient ? " AND client = ?" : "")
                + " GROUP BY provider_id, model ORDER BY SUM(req_count) DESC";
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            rows = db.queryList(sql,
                    rs -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("provider", rs.getString(1));
                        m.put("model", rs.getString(2));
                        m.put("requests", rs.getLong(3));
                        m.put("tokens", rs.getLong(4));
                        return m;
                    }, byClient ? new Object[]{fromDay, toDay, client} : new Object[]{fromDay, toDay});
        } catch (Exception e) {
            log.warn("[rank-trace] 实际用量查询失败: {}", e.toString());
        }
        return rows;
    }

    /** 窗口内出现过的来源工具（界面"按来源过滤"下拉的数据源；空串=旧数据未标注来源） */
    public List<String> usageClients(String fromDay, String toDay) {
        try {
            return db.queryList("SELECT DISTINCT client FROM usage_daily WHERE day BETWEEN ? AND ?"
                            + " ORDER BY client", rs -> rs.getString(1), fromDay, toDay);
        } catch (Exception e) {
            log.warn("[rank-trace] 来源列表查询失败: {}", e.toString());
            return new ArrayList<>();
        }
    }

    /**
     * 一次窗口内的观测汇总：应得份额均值 + 未进抽签席原因计数 + 样本数。
     * 界面必须说清这些数是"几次决策"观测出来的，否则 3 条样本的均值看着和 300 条一样可信。
     */
    public record ShareObservation(Map<String, Double> share, Map<String, Integer> bandOut,
                                   int samples, int fromDb, int fromMem) {}

    /** 采样库单次最多读多少条决策（均值不需要全量，也防止一次界面请求把几百 MB payload 读进内存） */
    private static final int SHARE_SCAN_MAX = 800;

    /**
     * 分摊对照的"应得"一侧：按窗口读采样库，内存环形只补齐库里没有的那些决策。
     *
     * <p>为什么不能只看内存环形：环形只有最近 200 次且重启即清空——网关一重启，
     * 界面就整列"暂无观测"，而库里的采样明明还在。采样是按整条决策无偏抽中的（默认 5%），
     * 所以份额均值口径与环形一致，只是样本更稀疏、覆盖更久。
     *
     * <p>单条快照最多落库 40 行候选（{@value #MAX_STORED_ROWS}），抽签席都在分数头部，
     * 截掉的是尾部，不影响份额与未进席原因的分类计数。
     */
    public ShareObservation observeShare(long fromTs, long toTs, String client, Integer maxTraces) {
        Map<String, double[]> acc = new LinkedHashMap<>();   // provider\nmodel → [份额和, 样本数]
        Map<String, Integer> bandOut = new LinkedHashMap<>();
        Set<String> counted = new HashSet<>();
        boolean byClient = client != null && !client.isBlank();
        int cap = maxTraces == null ? SHARE_SCAN_MAX : Math.max(1, Math.min(SHARE_SCAN_MAX, maxTraces));
        List<String> payloads = new ArrayList<>();
        try {
            String sql = "SELECT payload FROM rank_trace WHERE ts BETWEEN ? AND ?"
                    + (byClient ? " AND client = ?" : "")
                    + " ORDER BY ts DESC LIMIT ?";
            payloads = db.queryList(sql, rs -> rs.getString(1),
                    byClient ? new Object[]{fromTs, toTs, client, cap}
                            : new Object[]{fromTs, toTs, cap});
        } catch (Exception e) {
            log.warn("[rank-trace] 份额采样库查询失败: {}", e.toString());
        }
        int fromDb = 0;
        for (String payload : payloads) {
            if (payload == null || payload.isBlank()) continue;
            try {
                Map<String, Object> m = Json.parseObject(payload);
                String traceId = Json.str(m.get("traceId"));
                if (traceId != null) counted.add(traceId);
                if (accumulate(m.get("rows"), Boolean.TRUE.equals(m.get("loadShare")), acc, bandOut)) fromDb++;
            } catch (Exception e) {
                log.warn("[rank-trace] 份额 payload 解析失败: {}", e.toString());
            }
        }
        int fromMem = 0;
        synchronized (mem) {
            for (PolicyService.RankTrace t : mem) {
                if (t.ts < fromTs || t.ts > toTs) continue;
                if (byClient && !client.equalsIgnoreCase(String.valueOf(t.client))) continue;
                if (t.traceId != null && !counted.add(t.traceId)) continue;   // 库里已数过，别重复计
                if (accumulate(t.rows, t.loadShare, acc, bandOut)) fromMem++;
            }
        }
        Map<String, Double> out = new LinkedHashMap<>();
        acc.forEach((k, a) -> out.put(k, a[1] > 0 ? a[0] / a[1] : 0));
        return new ShareObservation(out, bandOut, fromDb + fromMem, fromDb, fromMem);
    }

    /**
     * 累加一条决策快照：抽签席份额进 {@code acc}，未进席原因码进 {@code bandOut}。
     * 行可以是内存里的 {@code Row}，也可以是库里 payload 解析出的 Map——两条路径同一套口径。
     *
     * @return 这条决策是否贡献了样本（有席或有出局原因）
     */
    private static boolean accumulate(Object rows, boolean loadShare,
                                      Map<String, double[]> acc, Map<String, Integer> bandOut) {
        if (!(rows instanceof Collection<?> col)) return false;
        int n = 0;
        for (Object o : col) {
            String provider, model, bandOutCode;
            boolean inBand;
            double share;
            if (o instanceof PolicyService.RankTrace.Row r) {
                provider = r.provider; model = r.model;
                inBand = r.inBand; share = r.share; bandOutCode = r.bandOut;
            } else if (o instanceof Map<?, ?> m) {
                provider = Json.str(m.get("provider")); model = Json.str(m.get("model"));
                inBand = Boolean.TRUE.equals(m.get("inBand"));
                share = m.get("share") instanceof Number num ? num.doubleValue() : 0;
                bandOutCode = Json.str(m.get("bandOut"));
            } else continue;
            if (inBand && share > 0) {
                double[] a = acc.computeIfAbsent(provider + "\n" + model, k -> new double[2]);
                a[0] += share;
                a[1] += 1;
                n++;
            } else if (loadShare && bandOutCode != null && !bandOutCode.isBlank()) {
                // 只在"分摊确实生效"的决策里数未进席：关着分摊时每条决策都无席，数出来是误导
                bandOut.merge(bandOutCode, 1, Integer::sum);
                n++;
            }
        }
        return n > 0;
    }
}
