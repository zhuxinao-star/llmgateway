package com.gw.store;

import com.gw.store.Database.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQLite DAO：当日额度读恢复、请求日志、用量聚合、运行时快照。
 * usage/request 均带 client 维度（接入工具）。
 */
@Service
public class StatsDao {

    private final Database db;
    public StatsDao(Database db) { this.db = db; }
    public Database getDb() { return db; }

    /** 启动时恢复某天的用量（内存计数器重建）；client='' 聚合到 entry 维度 */
    public List<UsageRow> loadUsageOfDay(String day) {
        String sql = "SELECT provider_id, model, req_count, tokens_in, tokens_out FROM usage_daily WHERE day = ? AND client = ''";
        try {
            return db.queryList(sql, rs -> new UsageRow(rs.getString(1), rs.getString(2), rs.getLong(3),
                    rs.getLong(4), rs.getLong(5)), day);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public record UsageRow(String providerId, String model, long reqCount, long tokensIn, long tokensOut) {}

    /** 单日全 client 用量（按工具视图/日趋势合计用） */
    public List<UsageRow> loadUsageOfDayAllClients(String day) {
        String sql = "SELECT provider_id, model, req_count, tokens_in, tokens_out FROM usage_daily WHERE day = ?";
        try {
            return db.queryList(sql, rs -> new UsageRow(rs.getString(1), rs.getString(2), rs.getLong(3),
                    rs.getLong(4), rs.getLong(5)), day);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** upsert 一天的用量计数（异步批量）；client 维度分桶（'' = entry 总额） */
    public void upsertUsage(String day, String providerId, String model, String client,
                            long reqDelta, long tokensInDelta, long tokensOutDelta) {
        String sql = """
            INSERT INTO usage_daily(day, provider_id, model, client, req_count, tokens_in, tokens_out, cost_real)
            VALUES(?,?,?,?,?,?,?,0)
            ON CONFLICT(day, provider_id, model, client) DO UPDATE SET
              req_count = req_count + excluded.req_count,
              tokens_in = tokens_in + excluded.tokens_in,
              tokens_out = tokens_out + excluded.tokens_out
            """;
        db.asyncWrite(sql, day, providerId, model, client, reqDelta, tokensInDelta, tokensOutDelta);
    }

    /** 按接入工具聚合（日历日区间 [from,to]）：请求数 + token；providers/models/clients 勾选时同口径收窄 */
    public List<ClientUsageRow> usageByClient(String from, String to,
                                              List<String> providers, List<String> models, List<String> clients) {
        List<String> where = new ArrayList<>(List.of("day BETWEEN ? AND ?", "client != ''"));
        List<Object> params = new ArrayList<>(List.of(from, to));
        addIn(where, params, "client", clients);
        addIn(where, params, "provider_id", providers);
        addIn(where, params, "model", models);
        String sql = "SELECT client, SUM(req_count), SUM(tokens_in), SUM(tokens_out) FROM usage_daily WHERE "
                + String.join(" AND ", where) + " GROUP BY client ORDER BY SUM(req_count) DESC";
        try {
            return db.queryList(sql, rs -> new ClientUsageRow(
                    rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getLong(4)), params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 用量汇总（日历日区间 [from,to]）：供应商×模型。未勾来源工具时看 client='' 总额桶；
     *  勾选后改看对应工具桶（每次请求同时写总额桶与工具桶，两者口径一致）。 */
    public List<Map<String, Object>> usageByProviderModel(String from, String to,
                                                          List<String> providers, List<String> models,
                                                          List<String> clients) {
        List<String> where = new ArrayList<>(List.of("day BETWEEN ? AND ?"));
        List<Object> params = new ArrayList<>(List.of(from, to));
        if (clients == null || clients.isEmpty()) where.add("client = ''");
        else addIn(where, params, "client", clients);
        addIn(where, params, "provider_id", providers);
        addIn(where, params, "model", models);
        String sql = "SELECT provider_id, model, SUM(req_count), SUM(tokens_in), SUM(tokens_out) FROM usage_daily WHERE "
                + String.join(" AND ", where) + " GROUP BY provider_id, model ORDER BY SUM(req_count) DESC";
        try {
            return db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("provider", rs.getString(1));
                m.put("model", rs.getString(2));
                m.put("requests", rs.getLong(3));
                m.put("tokensIn", rs.getLong(4));
                m.put("tokensOut", rs.getLong(5));
                return m;
            }, params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 日趋势（日历日区间 [from,to]）：按天聚合请求数与 token；过滤口径同用量汇总 */
    public List<Map<String, Object>> usageByDay(String from, String to,
                                                List<String> providers, List<String> models,
                                                List<String> clients) {
        List<String> where = new ArrayList<>(List.of("day BETWEEN ? AND ?"));
        List<Object> params = new ArrayList<>(List.of(from, to));
        if (clients == null || clients.isEmpty()) where.add("client = ''");
        else addIn(where, params, "client", clients);
        addIn(where, params, "provider_id", providers);
        addIn(where, params, "model", models);
        String sql = "SELECT day, SUM(req_count), SUM(tokens_in+tokens_out) FROM usage_daily WHERE "
                + String.join(" AND ", where) + " GROUP BY day ORDER BY day";
        try {
            return db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("day", rs.getString(1));
                m.put("requests", rs.getLong(2));
                m.put("tokens", rs.getLong(3));
                return m;
            }, params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 日趋势失败数（日历日区间 [from,to]）：request_log 里 status!='OK' 按天计数，
     *  口径同失败榜（provider_id/model_real/client 勾选时收窄，未勾 client=全量，对齐总额桶）。
     *  usage_daily 无失败列，故单独从明细表取，供 byDay 补失败率列。 */
    public List<Map<String, Object>> failuresByDay(String from, String to,
                                                   List<String> providers, List<String> models,
                                                   List<String> clients) {
        List<String> where = new ArrayList<>(List.of(
                "date(ts/1000,'unixepoch','localtime') BETWEEN ? AND ?", "status != 'OK'"));
        List<Object> params = new ArrayList<>(List.of(from, to));
        addIn(where, params, "provider_id", providers);
        addIn(where, params, "model_real", models);
        addIn(where, params, "client", clients);
        String sql = "SELECT date(ts/1000,'unixepoch','localtime') AS d, COUNT(*) FROM request_log WHERE "
                + String.join(" AND ", where) + " GROUP BY d ORDER BY d";
        try {
            return db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("day", rs.getString(1));
                m.put("failures", rs.getLong(2));
                return m;
            }, params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 按接入工具聚合（精确单日）：请求数 + token */
    public List<ClientUsageRow> usageByClientOfDay(String day) {
        String sql = """
            SELECT client, SUM(req_count), SUM(tokens_in), SUM(tokens_out)
            FROM usage_daily
            WHERE day = ? AND client != ''
            GROUP BY client ORDER BY SUM(req_count) DESC
            """;
        try {
            return db.queryList(sql, rs -> new ClientUsageRow(
                    rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getLong(4)), day);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 按接入工具统计失败次数（日历日区间 [from,to]，与用量统计同口径；status != OK 全算失败，含超时/中断） */
    public List<Map<String, Object>> failuresByClient(String from, String to,
                                                      List<String> providers, List<String> models,
                                                      List<String> clients) {
        List<String> where = new ArrayList<>(List.of(
                "date(ts/1000,'unixepoch','localtime') BETWEEN ? AND ?", "status != 'OK'"));
        List<Object> params = new ArrayList<>(List.of(from, to));
        addIn(where, params, "client", clients);
        addIn(where, params, "provider_id", providers);
        addIn(where, params, "model_real", models);
        String sql = "SELECT client, COUNT(*), SUM(CASE WHEN status='STREAM_TIMEOUT' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN status='STREAM_ABORTED' THEN 1 ELSE 0 END) FROM request_log WHERE "
                + String.join(" AND ", where) + " GROUP BY client ORDER BY COUNT(*) DESC";
        try {
            return db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("client", rs.getString(1));
                m.put("failures", rs.getLong(2));
                m.put("timeouts", rs.getLong(3));
                m.put("errors", rs.getLong(4));
                m.put("aborted", rs.getLong(5));
                return m;
            }, params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 按供应商×模型统计失败次数（日历日区间 [from,to]，与用量汇总同维度） */
    public List<Map<String, Object>> failuresByProviderModel(String from, String to,
                                                             List<String> providers, List<String> models,
                                                             List<String> clients) {
        List<String> where = new ArrayList<>(List.of(
                "date(ts/1000,'unixepoch','localtime') BETWEEN ? AND ?", "status != 'OK'",
                "provider_id IS NOT NULL", "provider_id != '-'"));
        List<Object> params = new ArrayList<>(List.of(from, to));
        addIn(where, params, "provider_id", providers);
        addIn(where, params, "model_real", models);
        addIn(where, params, "client", clients);
        String sql = "SELECT provider_id, model_real, COUNT(*), SUM(CASE WHEN status='STREAM_TIMEOUT' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) FROM request_log WHERE "
                + String.join(" AND ", where) + " GROUP BY provider_id, model_real ORDER BY COUNT(*) DESC LIMIT 200";
        try {
            return db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("provider", rs.getString(1));
                m.put("model", rs.getString(2));
                m.put("failures", rs.getLong(3));
                m.put("timeouts", rs.getLong(4));
                m.put("errors", rs.getLong(5));
                return m;
            }, params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
    }

    public record ClientUsageRow(String client, long reqCount, long tokensIn, long tokensOut) {}

    /**
     * 模型自动停用器的证据来源：近 fromMs 之后，按 (provider_id, model_real) 聚合真实流量结果。
     * 只统计真实客户端（排除 probe/selfcheck/reconcile 背景流量，避免自反馈）。
     * 返回每行 {provider, model, notFound, quota, ok}：
     *   notFound=MODEL_NOT_SUPPORTED 次数，quota=QUOTA 次数，ok=成功次数。
     */
    public List<Map<String, Object>> recentModelOutcomes(long fromMs) {
        String sql = "SELECT provider_id, model_real,"
                + " SUM(CASE WHEN error_class='MODEL_NOT_SUPPORTED' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN error_class='QUOTA' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END)"
                + " FROM request_log WHERE ts >= ?"
                + " AND provider_id IS NOT NULL AND provider_id != '-'"
                + " AND (client IS NULL OR client NOT IN ('probe','selfcheck','reconcile'))"
                + " GROUP BY provider_id, model_real";
        try {
            return db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("provider", rs.getString(1));
                m.put("model", rs.getString(2));
                m.put("notFound", rs.getLong(3));
                m.put("quota", rs.getLong(4));
                m.put("ok", rs.getLong(5));
                return m;
            }, fromMs);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /**
     * 按 Key（渠道）聚合窗口内的真实转发量：一家配了多把 Key 时，回答"每一把分别扛了多少、撞了多少 429"。
     * channel_id 为 NULL 的历史行（本功能上线之前）单独成桶，由界面标成"上线前无归属"，
     * 绝不摊给任何一把在用的 Key。网关侧拒绝（REJECTED）没有供应商也没有 Key，不进这张表。
     */
    public List<Map<String, Object>> keyUsage(long fromMs, long toMs, List<String> providers,
                                              List<String> clients, List<String> models) {
        List<String> where = new ArrayList<>(List.of(
                "ts BETWEEN ? AND ?", "provider_id IS NOT NULL", "provider_id != '-'"));
        List<Object> params = new ArrayList<>(List.of(fromMs, toMs));
        addIn(where, params, "provider_id", providers);
        addIn(where, params, "client", clients);
        addIn(where, params, "model_real", models);
        String sql = "SELECT provider_id, model_real, COALESCE(channel_id,''), COUNT(*),"
                + " SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN status!='OK' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN error_class='RATE_LIMIT' THEN 1 ELSE 0 END),"
                + " SUM(COALESCE(tokens_in,0)), SUM(COALESCE(tokens_out,0)),"
                + " SUM(CASE WHEN status='STREAM_TIMEOUT' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END),"
                + " SUM(CASE WHEN status='STREAM_ABORTED' THEN 1 ELSE 0 END)"
                + " FROM request_log WHERE " + String.join(" AND ", where)
                + " GROUP BY provider_id, model_real, COALESCE(channel_id,'')"
                + " ORDER BY COUNT(*) DESC LIMIT 300";
        List<Map<String, Object>> rows;
        try {
            rows = db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("provider", rs.getString(1));
                m.put("model", rs.getString(2));
                m.put("channel", rs.getString(3));
                m.put("requests", rs.getLong(4));
                m.put("ok", rs.getLong(5));
                m.put("failures", rs.getLong(6));
                m.put("rateLimits", rs.getLong(7));
                m.put("tokensIn", rs.getLong(8));
                m.put("tokensOut", rs.getLong(9));
                // 三个细分与 failuresByClient/ByProviderModel 同口径：只为徽标 tooltip 与钻取预置状态，
                // 不是 failures 的完整拆分（还有 CLIENT_ABORTED 等少数状态只计入总数）
                m.put("timeouts", rs.getLong(10));
                m.put("errors", rs.getLong(11));
                m.put("aborted", rs.getLong(12));
                m.put("ttftMedianMs", null);
                return m;
            }, params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
        if (rows.isEmpty()) return rows;
        // 中位数交给 Java 侧算（SQLite 无 MEDIAN；与 recentOkTtfts/分摊页同一套口径，避免两套中位数打架）
        List<String> medianWhere = new ArrayList<>(List.of(
                "ts BETWEEN ? AND ?", "status='OK'", "ttft_ms IS NOT NULL",
                "provider_id IS NOT NULL", "provider_id != '-'"));
        List<Object> medianParams = new ArrayList<>(List.of(fromMs, toMs));
        addIn(medianWhere, medianParams, "provider_id", providers);
        addIn(medianWhere, medianParams, "client", clients);
        addIn(medianWhere, medianParams, "model_real", models);
        Map<String, List<Integer>> ttfts = new HashMap<>();
        try {
            db.queryList("SELECT provider_id, model_real, COALESCE(channel_id,''), ttft_ms FROM request_log WHERE "
                            + String.join(" AND ", medianWhere) + " ORDER BY ttft_ms LIMIT 50000",
                    rs -> {
                        ttfts.computeIfAbsent(rs.getString(1) + "\n" + rs.getString(2) + "\n" + rs.getString(3),
                                k -> new ArrayList<>()).add(rs.getInt(4));
                        return Boolean.TRUE;
                    }, medianParams.toArray());
        } catch (SQLException e) {
            return rows;   // 中位数取不到不影响主表：列留空由界面显示"-"
        }
        for (Map<String, Object> r : rows) {
            List<Integer> list = ttfts.get(r.get("provider") + "\n" + r.get("model") + "\n" + r.get("channel"));
            if (list == null || list.isEmpty()) continue;
            r.put("ttftMedianMs", median(list));
        }
        return rows;
    }

    /** 已升序列表的中位数：偶数取中间两值均值（与分摊页/TTFT 门槛同一算法） */
    private static long median(List<Integer> sorted) {
        int n = sorted.size();
        if (n % 2 == 1) return sorted.get(n / 2);
        return Math.round((sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0);
    }

    public void insertRequestLog(long ts, String modelReq, String providerId, String modelReal,
                                  Integer ttftMs, Integer totalMs, Long tokensIn, Long tokensOut,
                                  String status, int attempts, String profile, String error, String client,
                                  String compress, String traceId, Integer httpStatus, String errorClass,
                                  String channelId) {
        String sql = """
            INSERT INTO request_log(ts, model_req, provider_id, model_real, ttft_ms, total_ms,
              tokens_in, tokens_out, cost_real, status, attempts, profile, error, client, compress,
              trace_id, http_status, error_class, channel_id)
            VALUES(?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,?,?,?,?)
            """;
        db.asyncWrite(sql, ts, modelReq, providerId, modelReal, ttftMs, totalMs, tokensIn, tokensOut,
                status, attempts, profile, error, client, compress, traceId, httpStatus, errorClass,
                channelId == null || channelId.isBlank() ? null : channelId);
    }

    public void cleanupOldLogs(int keepDays) {
        long cutoff = System.currentTimeMillis() - keepDays * 24L * 3600 * 1000;
        db.asyncWrite("DELETE FROM request_log WHERE ts < ?", cutoff);
    }

    /** 保存运行时快照（条目行 channel_id='*'；Key 冷却行存具体 channel_id） */
    public void saveRuntimeSnapshot(Iterable<SnapshotRow> rows) {
        String sql = """
            INSERT INTO runtime_snapshot(provider_id, channel_id, model, ewma_ttft_ms, state,
              cool_down_until, consecutive_failures, ewma_tokens_per_sec, recent_ttft_ms, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(provider_id, channel_id, model) DO UPDATE SET
              ewma_ttft_ms=excluded.ewma_ttft_ms, state=excluded.state,
              cool_down_until=excluded.cool_down_until,
              consecutive_failures=excluded.consecutive_failures,
              ewma_tokens_per_sec=excluded.ewma_tokens_per_sec,
              recent_ttft_ms=excluded.recent_ttft_ms, updated_at=excluded.updated_at
            """;
        try {
            for (SnapshotRow r : rows) db.syncWrite(sql, r.providerId, r.channelId, r.model,
                    r.ewmaTtftMs, r.state, r.coolDownUntil, r.consecutiveFailures,
                    r.ewmaTokensPerSec, r.recentTtftMedian, System.currentTimeMillis());
        } catch (SQLException ignore) {
        }
    }

    public record SnapshotRow(String providerId, String channelId, String model,
                              Double ewmaTtftMs, String state, Long coolDownUntil,
                              Integer consecutiveFailures, Double ewmaTokensPerSec,
                              Double recentTtftMedian, Long updatedAt) {
        /** 持久化侧不需要回读时间戳，给个不带 updated_at 的构造口子（写入时由 DB 现算）。
         *  必须 public：PolicyService 在另一个包（com.gw.policy）里构造它。 */
        public SnapshotRow(String providerId, String channelId, String model, Double ewmaTtftMs, String state,
                           Long coolDownUntil, Integer consecutiveFailures, Double ewmaTokensPerSec,
                           Double recentTtftMedian) {
            this(providerId, channelId, model, ewmaTtftMs, state, coolDownUntil, consecutiveFailures,
                    ewmaTokensPerSec, recentTtftMedian, null);
        }
    }

    /** 最近 N 条成功请求的 TTFT（常规上下文）：进程重启后给分摊抽签门槛/速度系数补"最近实测"样本。
     *  三条口径与实时评分侧保持一致，否则回填会把实时侧刻意排除的东西又塞回来：
     *   - 排除 client='probe'（巡检 ping 不代表真实请求速度，实时侧 recordSuccess 同样不进窗）；
     *   - 排除 ttft_ms < minTtftMs 的测量伪值（修复前流式路径把首字记成 0~3ms）；
     *   - 模型按大小写不敏感匹配（同一模型的不同拼法共用一份速度历史，别把样本切两半）。 */
    public List<Long> recentOkTtfts(String providerId, String model, int limit, long maxInputTokens, long minTtftMs) {
        String sql = "SELECT ttft_ms FROM request_log WHERE provider_id=? AND lower(model_real)=lower(?) AND status='OK' " +
                "AND ttft_ms IS NOT NULL AND ttft_ms >= ? AND (client IS NULL OR client NOT IN ('probe','selfcheck','reconcile')) " +
                "AND (tokens_in IS NULL OR tokens_in <= ?) ORDER BY id DESC LIMIT ?";
        try {
            return db.queryList(sql, rs -> rs.getLong(1), providerId, model, minTtftMs, maxInputTokens, limit);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 慢性限流观测：近窗口内每家（跨所有 Key、真实流量）的尝试数与 429 数。
     *  限流学习是内存态（重启清零、6 小时无流量淡出），而"这家长期被限流"是库里的既成事实；
     *  这一路只读库，让降权/撤席在重启后立刻恢复，不必重新撞墙才知道。
     *  隔离客户端（巡检/体检/确认探测）不算：它们不代表用户真实流量对额度的占用。 */
    public Map<String, long[]> chronicRateLimits(long sinceTs) {
        String sql = "SELECT provider_id, COUNT(*), SUM(CASE WHEN error_class='RATE_LIMIT' THEN 1 ELSE 0 END) " +
                "FROM request_log WHERE ts >= ? AND (client IS NULL OR client NOT IN ('probe','selfcheck','reconcile')) " +
                "GROUP BY provider_id";
        Map<String, long[]> out = new HashMap<>();
        try {
            db.queryList(sql, rs -> {
                out.put(rs.getString(1), new long[]{rs.getLong(2), rs.getLong(3)});
                return null;
            }, sinceTs);
        } catch (SQLException e) {
            return Map.of();
        }
        return out;
    }

    public List<SnapshotRow> loadRuntimeSnapshots() {
        String sql = "SELECT provider_id, channel_id, model, ewma_ttft_ms, state, cool_down_until, consecutive_failures, ewma_tokens_per_sec, recent_ttft_ms, updated_at FROM runtime_snapshot";
        try {
            return db.queryList(sql, rs -> new SnapshotRow(rs.getString(1), rs.getString(2), rs.getString(3),
                    (Double) rs.getObject(4), rs.getString(5), rs.getLong(6), rs.getInt(7),
                    (Double) rs.getObject(8), (Double) rs.getObject(9), rs.getLong(10)));
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 清掉历史遗留的脏快照行：channel_id 里累积的命名空间标记（每次重启多一个 \u0001，
     *  永远匹配不到真实渠道，Key 冷却跨重启恢复因此形同虚设），以及同一模型按大小写分裂成的
     *  旧行（新写入一律小写归一，旧行不再会被读到，留着只会让表膨胀）。
     *  @return 删除行数（诊断/日志用） */
    public int cleanupRuntimeSnapshotJunk() {
        String pred = "instr(channel_id, char(1)) > 0 OR (model <> '*' AND model <> lower(model))";
        try {
            List<Integer> n = db.queryList("SELECT COUNT(*) FROM runtime_snapshot WHERE " + pred,
                    rs -> rs.getInt(1));
            db.syncWrite("DELETE FROM runtime_snapshot WHERE " + pred);
            return n.isEmpty() ? 0 : n.get(0);
        } catch (SQLException e) {
            return 0;
        }
    }

    /** 管理页：请求日志分页 + 合计（多维过滤：请求名/真实模型/供应商 LIKE；status 精确/statusIn 多选；日期区间 [from,to]） */
    public List<LogRow> pageLogs(int offset, int limit, LogFilter f) {
        return pageLogs(offset, limit, f, null, null);
    }

    /** #16 表头可排序：sort 走列白名单（非法值回落 id DESC），dir 仅认 asc。杜绝拼接注入。 */
    public List<LogRow> pageLogs(int offset, int limit, LogFilter f, String sort, String dir) {
        List<String> where = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        addDateRange(where, params, f.dayFrom, f.dayTo);
        // 多值维度：勾选了值走 IN 精确匹配；没勾选时可回退到手输关键字 LIKE（单选框保留输入能力）
        addInOrLike(where, params, "model_req", f.models, f.model);
        addInOrLike(where, params, "model_real", f.realModels, f.realModel);
        addInOrLike(where, params, "provider_id", f.providers, f.provider);
        addInOrLike(where, params, "client", f.clients, f.client);
        addInOrLike(where, params, "channel_id", f.channels, f.channel);
        if (f.statusIn != null && !f.statusIn.isEmpty()) {
            where.add("status IN (" + String.join(",", java.util.Collections.nCopies(f.statusIn.size(), "?")) + ")");
            params.addAll(f.statusIn);
        } else if (f.status != null && !f.status.isBlank()) { where.add("status = ?"); params.add(f.status); }
        RowMapper<LogRow> mapper = LOG_MAPPER;
        String sql = SELECT_LOGS;
        if (!where.isEmpty()) sql += " WHERE " + String.join(" AND ", where);
        sql += " ORDER BY " + resolveLogSort(sort) + ("asc".equalsIgnoreCase(dir) ? " ASC" : " DESC")
             + " LIMIT ? OFFSET ?";
        params.add(limit);
        params.add(offset);
        try {
            return db.queryList(sql, mapper, params.toArray());
        } catch (SQLException e) {
            return List.of();
        }
    }

    private static final String SELECT_LOGS =
            "SELECT ts, model_req, provider_id, model_real, ttft_ms, total_ms, tokens_in, tokens_out, status, "
          + "attempts, profile, error, client, compress, trace_id, http_status, error_class, channel_id FROM request_log";

    /** #16 排序列白名单：前端 key → 真实 SQL 列。未命中一律 id（时间序），杜绝注入。 */
    private static String resolveLogSort(String sort) {
        if (sort == null) return "ts";
        return switch (sort) {
            case "time", "ts" -> "ts";
            case "client" -> "client";
            case "model" -> "model_req";
            case "provider" -> "provider_id";
            case "realModel" -> "model_real";
            case "key" -> "channel_id";
            case "ttft" -> "ttft_ms";
            case "total" -> "total_ms";
            case "tokens" -> "tokens_in";
            case "status" -> "status";
            case "attempts" -> "attempts";
            default -> "id";
        };
    }

    private static final RowMapper<LogRow> LOG_MAPPER = rs -> new LogRow(
            rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4),
            rs.getObject(5) == null ? null : rs.getInt(5),
            rs.getObject(6) == null ? null : rs.getInt(6),
            rs.getLong(7), rs.getLong(8), rs.getString(9), rs.getInt(10),
            rs.getString(11), rs.getString(12), rs.getString(13), rs.getString(14),
            rs.getString(15), rs.getObject(16) == null ? null : rs.getInt(16), rs.getString(17),
            rs.getString(18));

    /** 一次请求（trace_id）的全链路行，按 ts 升序：FAILED→FAILED→OK 的时间线还原 */
    public List<LogRow> logsByTrace(String traceId) {
        if (traceId == null || traceId.isBlank()) return List.of();
        try {
            return db.queryList(SELECT_LOGS + " WHERE trace_id=? ORDER BY ts ASC, id ASC",
                    LOG_MAPPER, traceId);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 日志过滤条件（全部可空；XxxIn 多值勾选优先，未勾选时 xxx 单值走 LIKE；
     *  statusIn 多状态时优先于 status 单状态；dayFrom/dayTo 日历日区间闭区间，单填一个即单边；
     *  channel/channels = Key（渠道）归属，channel_id 为 NULL 的旧行不属于任何一把 Key） */
    public record LogFilter(String model, String realModel, String provider, String status,
                            List<String> statusIn, String client, String day,
                            List<String> models, List<String> realModels,
                            List<String> providers, List<String> clients,
                            String channel, List<String> channels,
                            String dayFrom, String dayTo) {

        public LogFilter(String model, String realModel, String provider, String status,
                         List<String> statusIn, String client, String day,
                         List<String> models, List<String> realModels,
                         List<String> providers, List<String> clients,
                         String channel, List<String> channels) {
            this(model, realModel, provider, status, statusIn, client, day,
                    models, realModels, providers, clients, channel, channels, day, day);
        }
    }

    /** 日期区间条件：两边都有=区间；只有一边=单边；都没有=不过滤。day 字段作为兼容入口（from=to=day） */
    private static void addDateRange(List<String> where, List<Object> params, String from, String to) {
        boolean hasFrom = from != null && !from.isBlank();
        boolean hasTo = to != null && !to.isBlank();
        if (hasFrom && hasTo) {
            where.add("date(ts/1000,'unixepoch','localtime') BETWEEN ? AND ?");
            params.add(from);
            params.add(to);
        } else if (hasFrom) {
            where.add("date(ts/1000,'unixepoch','localtime') >= ?");
            params.add(from);
        } else if (hasTo) {
            where.add("date(ts/1000,'unixepoch','localtime') <= ?");
            params.add(to);
        }
    }

    /** 当前过滤条件下的合计（行数/成功/失败/tokens）——日志页表底合计行 */
    public Map<String, Object> logsSummary(LogFilter f) {
        List<String> where = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        addDateRange(where, params, f.dayFrom(), f.dayTo());
        addInOrLike(where, params, "model_req", f.models(), f.model());
        addInOrLike(where, params, "model_real", f.realModels(), f.realModel());
        addInOrLike(where, params, "provider_id", f.providers(), f.provider());
        addInOrLike(where, params, "client", f.clients(), f.client());
        addInOrLike(where, params, "channel_id", f.channels(), f.channel());
        if (f.statusIn() != null && !f.statusIn().isEmpty()) {
            where.add("status IN (" + String.join(",", java.util.Collections.nCopies(f.statusIn().size(), "?")) + ")");
            params.addAll(f.statusIn());
        } else if (f.status() != null && !f.status().isBlank()) {
            where.add("status = ?");
            params.add(f.status());
        }
        String sql = "SELECT COUNT(*), SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END), " +
                "SUM(CASE WHEN status!='OK' THEN 1 ELSE 0 END), " +
                "SUM(COALESCE(tokens_in,0)), SUM(COALESCE(tokens_out,0)) FROM request_log";
        if (!where.isEmpty()) sql += " WHERE " + String.join(" AND ", where);
        try {
            return db.queryList(sql, rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("total", rs.getLong(1));
                m.put("ok", rs.getLong(2));
                m.put("failed", rs.getLong(3));
                m.put("tokensIn", rs.getLong(4));
                m.put("tokensOut", rs.getLong(5));
                return m;
            }, params.toArray()).stream().findFirst().orElse(Map.of());
        } catch (SQLException e) {
            return Map.of();
        }
    }

    /** 多值列表：勾选值 IN 精确匹配；否则回退手输关键字 LIKE（两空则不过滤） */
    private static void addInOrLike(List<String> where, List<Object> params, String column,
                                    List<String> inValues, String likeKeyword) {
        if (inValues != null && !inValues.isEmpty()) {
            where.add(column + " IN (" + String.join(",", java.util.Collections.nCopies(inValues.size(), "?")) + ")");
            params.addAll(inValues);
        } else if (likeKeyword != null && !likeKeyword.isBlank()) {
            where.add(column + " LIKE ?");
            params.add("%" + likeKeyword + "%");
        }
    }

    /** 多值 IN 过滤（空/未勾选 = 不加条件）；列名只来自内部调用点，值走占位符 */
    private static void addIn(List<String> where, List<Object> params, String column, List<String> values) {
        if (values != null && !values.isEmpty()) {
            where.add(column + " IN (" + String.join(",", java.util.Collections.nCopies(values.size(), "?")) + ")");
            params.addAll(values);
        }
    }

    /** 用量页下拉选项：usage_daily 里出现过的维度值（与用量表同源，保证选项与展示值一致） */
    public List<String> usageDimensionValues(String type) {
        String col = switch (type) {
            case "usage_provider" -> "provider_id";
            case "usage_model" -> "model";
            case "usage_client" -> "client";
            default -> null;
        };
        if (col == null) return List.of();
        String sql = "SELECT DISTINCT " + col + " FROM usage_daily"
                + ("client".equals(col) ? " WHERE client != ''" : "")
                + " ORDER BY " + col + " LIMIT 500";
        try {
            return db.queryList(sql, rs -> rs.getString(1));
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** 日志里出现过的维度值（下拉选项）：type = model_req | model_real | provider_id | client | status | channel_id */
    public List<String> logDimensionValues(String type) {
        Set<String> allowed = Set.of("model_req", "model_real", "provider_id", "client", "status", "channel_id");
        if (!allowed.contains(type)) return List.of();
        try {
            return db.queryList("SELECT DISTINCT " + type + " FROM request_log WHERE " + type
                            + " IS NOT NULL ORDER BY " + type + " LIMIT 500",
                    rs -> rs.getString(1));
        } catch (SQLException e) {
            return List.of();
        }
    }

    public record LogRow(long ts, String modelReq, String providerId, String modelReal,
                         Integer ttftMs, Integer totalMs, long tokensIn, long tokensOut,
                         String status, int attempts, String profile, String error, String client,
                         String compress, String traceId, Integer httpStatus, String errorClass,
                         String channelId) {}
}
