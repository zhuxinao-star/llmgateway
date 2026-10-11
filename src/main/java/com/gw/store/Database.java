package com.gw.store;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * SQLite 访问层（WAL 模式）。所有写操作经过内存队列异步批量落库，
 * 读操作直接查询。单文件嵌入式，无需安装。
 */
@Service
public class Database implements AutoCloseable {

    private final String url;
    private final Connection writeConn;   // 单写连接（SQLite 推荐）
    private final BlockingQueue<WriteOp> writeQueue = new LinkedBlockingQueue<>(100_000);
    private Thread writerThread;
    private volatile boolean closed = false;

    public Database(@Value("${gateway.db-path:}") String dbPathProp,
                    @Value("${gateway.config-path:config/config.yaml}") String configPath) throws Exception {
        String path = (dbPathProp != null && !dbPathProp.isBlank()) ? dbPathProp : resolveDefaultDbPath(configPath);
        Path p = Path.of(path).toAbsolutePath();
        Files.createDirectories(p.getParent() == null ? Path.of(".") : p.getParent());
        this.url = "jdbc:sqlite:" + p;
        this.writeConn = DriverManager.getConnection(url);
        try (Statement st = writeConn.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA synchronous=NORMAL");
            st.execute("PRAGMA busy_timeout=5000");
            st.execute("PRAGMA foreign_keys=ON");
        }
        initSchema();
        startWriter();
    }

    private String resolveDefaultDbPath(String configPath) {
        Path cfg = Path.of(configPath).toAbsolutePath().getParent();
        return cfg.resolve("data/llm-gateway.db").toString();
    }

    private void initSchema() throws SQLException {
        String ddl = """
            CREATE TABLE IF NOT EXISTS usage_daily (
              day TEXT, provider_id TEXT, model TEXT, client TEXT DEFAULT '',
              req_count INTEGER DEFAULT 0, tokens_in INTEGER DEFAULT 0,
              tokens_out INTEGER DEFAULT 0, cost_real REAL DEFAULT 0,
              PRIMARY KEY(day, provider_id, model, client));
            CREATE TABLE IF NOT EXISTS request_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER,
              model_req TEXT, provider_id TEXT, model_real TEXT,
              ttft_ms INTEGER, total_ms INTEGER, tokens_in INTEGER, tokens_out INTEGER,
              cost_real REAL, status TEXT, attempts INTEGER, profile TEXT, error TEXT, client TEXT DEFAULT '');
            CREATE INDEX IF NOT EXISTS idx_log_ts ON request_log(ts);
            CREATE TABLE IF NOT EXISTS runtime_snapshot (
              provider_id TEXT, channel_id TEXT DEFAULT '*',
              model TEXT, ewma_ttft_ms REAL, state TEXT,
              cool_down_until INTEGER, consecutive_failures INTEGER, ewma_tokens_per_sec REAL, updated_at INTEGER,
              PRIMARY KEY(provider_id, channel_id, model));
            CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
            CREATE TABLE IF NOT EXISTS rank_trace (
              id INTEGER PRIMARY KEY AUTOINCREMENT, trace_id TEXT, ts INTEGER,
              requested_model TEXT, client TEXT, endpoint TEXT, strategy TEXT,
              has_tools INTEGER, picked_provider TEXT, picked_model TEXT, pick_reason TEXT,
              n_rows INTEGER, n_filtered INTEGER, payload TEXT);
            CREATE INDEX IF NOT EXISTS idx_rank_trace_ts ON rank_trace(ts);
            CREATE INDEX IF NOT EXISTS idx_rank_trace_trace ON rank_trace(trace_id);
            """;
        try (Statement st = writeConn.createStatement()) {
            for (String sql : ddl.split(";\n")) {
                String s = sql.strip();
                if (!s.isEmpty()) st.execute(s);
            }
        }
        migrate();
    }

    /** 轻量迁移：老库补 client 维度（幂等，探测列名/主键） */
    private void migrate() {
        try {
            // request_log：仅加列
            if (!columnExists("request_log", "client")) {
                writeConn.createStatement().execute("ALTER TABLE request_log ADD COLUMN client TEXT DEFAULT ''");
            }
            if (!columnExists("request_log", "compress")) {
                writeConn.createStatement().execute("ALTER TABLE request_log ADD COLUMN compress TEXT DEFAULT ''");
            }
            // usage_daily：client 要进主键（分桶维度），SQLite 不能改 PK → 重建表
            if (!columnExists("usage_daily", "client")) {
                writeConn.setAutoCommit(false);
                try {
                    Statement st = writeConn.createStatement();
                    st.execute("""
                        CREATE TABLE usage_daily_new (
                          day TEXT, provider_id TEXT, model TEXT, client TEXT DEFAULT '',
                          req_count INTEGER DEFAULT 0, tokens_in INTEGER DEFAULT 0,
                          tokens_out INTEGER DEFAULT 0, cost_real REAL DEFAULT 0,
                          PRIMARY KEY(day, provider_id, model, client))""");
                    st.execute("INSERT INTO usage_daily_new(day, provider_id, model, client, req_count, tokens_in, tokens_out, cost_real) " +
                            "SELECT day, provider_id, model, '' AS client_col, req_count, tokens_in, tokens_out, cost_real FROM usage_daily");
                    st.execute("DROP TABLE usage_daily");
                    st.execute("ALTER TABLE usage_daily_new RENAME TO usage_daily");
                    writeConn.commit();
                } catch (SQLException e) {
                    try { writeConn.rollback(); } catch (SQLException ignore) {}
                    System.err.println("[migrate] usage_daily 重建失败: " + e.getMessage());
                } finally {
                    writeConn.setAutoCommit(true);
                }
            }
            // runtime_snapshot：v7.6.3 补吞吐列（重启恢复 T 因子样本，防无样本档给默认分）
            if (!columnExists("runtime_snapshot", "ewma_tokens_per_sec")) {
                writeConn.createStatement().execute(
                        "ALTER TABLE runtime_snapshot ADD COLUMN ewma_tokens_per_sec REAL");
            }
            // runtime_snapshot：v8.3 补最近 TTFT 中位数列（分摊抽签门槛样本；重启不回归 EWMA 尖峰误判）
            if (!columnExists("runtime_snapshot", "recent_ttft_ms")) {
                writeConn.createStatement().execute(
                        "ALTER TABLE runtime_snapshot ADD COLUMN recent_ttft_ms REAL");
            }
            // 第3批（#10 + trace 同一次迁移）：request_log 补三列 + trace 索引
            //   trace_id   —— 一次客户端请求的多 attempt 行串联（旧行 NULL，UI 显示"无链路"）
            //   http_status—— FAILED 区分 429/400/500（以前只落 status=FAILED）
            //   error_class—— UpstreamErrorClassifier 语义分类，文字型限流也可见可筛
            if (!columnExists("request_log", "trace_id")) {
                writeConn.createStatement().execute(
                        "ALTER TABLE request_log ADD COLUMN trace_id TEXT");
            }
            if (!columnExists("request_log", "http_status")) {
                writeConn.createStatement().execute(
                        "ALTER TABLE request_log ADD COLUMN http_status INTEGER");
            }
            if (!columnExists("request_log", "error_class")) {
                writeConn.createStatement().execute(
                        "ALTER TABLE request_log ADD COLUMN error_class TEXT");
            }
            // request_log 补 Key 归属列：一家多把 Key 时要能回答"这一发用的是哪把"。
            //   旧行留 NULL（不是 ''），界面据此显示"上线前无归属"，不能把它当成某把 Key 的量。
            //   不额外建索引：按 Key 的统计仍先按 ts 走 idx_log_ts 收窄窗口，避免给单写连接加写放大。
            if (!columnExists("request_log", "channel_id")) {
                writeConn.createStatement().execute(
                        "ALTER TABLE request_log ADD COLUMN channel_id TEXT");
            }
            writeConn.createStatement().execute(
                    "CREATE INDEX IF NOT EXISTS idx_request_log_trace ON request_log(trace_id)");
        } catch (SQLException e) {
            System.err.println("[migrate] 迁移探测失败: " + e.getMessage());
        }
    }

    private boolean columnExists(String table, String column) throws SQLException {
        try (ResultSet rs = writeConn.createStatement()
                .executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                if (column.equalsIgnoreCase(rs.getString("name"))) return true;
            }
        }
        return false;
    }

    // ---------- 异步写 ----------
    private record WriteOp(String sql, Object[] params, Consumer<Boolean> ack) {}

    private void startWriter() {
        writerThread = Thread.ofVirtual().name("gw-db-writer").start(() -> {
            List<WriteOp> batch = new ArrayList<>();
            while (!closed) {
                try {
                    WriteOp first = writeQueue.poll(1, TimeUnit.SECONDS);
                    if (first == null) continue;
                    batch.clear();
                    batch.add(first);
                    writeQueue.drainTo(batch, 99);
                    try {
                        writeConn.setAutoCommit(false);
                        // 与 syncWrite 共用连接锁：避免同步写与异步批次并发交错（SQLite 单连接非线程安全）
                        synchronized (writeConn) {
                            for (WriteOp op : batch) {
                                try (PreparedStatement ps = writeConn.prepareStatement(op.sql())) {
                                    bind(ps, op.params());
                                    ps.executeUpdate();
                                }
                            }
                            writeConn.commit();
                        }
                        for (WriteOp op : batch) { if (op.ack() != null) op.ack().accept(true); }
                    } catch (SQLException e) {
                        try { writeConn.rollback(); } catch (SQLException ignore) {}
                        for (WriteOp op : batch) { if (op.ack() != null) op.ack().accept(false); }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
    }

    /** 提交一条异步写（INSERT/UPDATE 语句由调用方构造） */
    public void asyncWrite(String sql, Object... params) {
        if (!closed) writeQueue.offer(new WriteOp(sql, params, null));
    }

    /** 同步写（schema 变更、快照等低频操作）。
     *  自带 commit：写线程会把连接留在 autoCommit=false，若只执行不提交，
     *  数据会困在未提交事务里，直到下一条请求的异步批次顺带刷库——
     *  空闲网关的快照因此永远落不了盘（重启恢复恰好只发生在空闲/停机之后）。 */
    public void syncWrite(String sql, Object... params) throws SQLException {
        synchronized (writeConn) {
            try (PreparedStatement ps = writeConn.prepareStatement(sql)) {
                bind(ps, params);
                ps.executeUpdate();
                writeConn.commit();
            }
        }
    }

    public interface RowMapper<T> { T map(ResultSet rs) throws SQLException; }

    /** 单行查询：返回第一行映射结果；无行返回 null */
    public <T> T queryOne(String sql, RowMapper<T> mapper, Object... params) throws SQLException {
        synchronized (writeConn) {
            try (PreparedStatement ps = writeConn.prepareStatement(sql)) {
                bind(ps, params);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return mapper.map(rs);
                    return null;
                }
            }
        }
    }

    /** 列表查询：所有行映射成 List */
    public <T> List<T> queryList(String sql, RowMapper<T> mapper, Object... params) throws SQLException {
        synchronized (writeConn) {
            List<T> out = new ArrayList<>();
            try (PreparedStatement ps = writeConn.prepareStatement(sql)) {
                bind(ps, params);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) out.add(mapper.map(rs));
                }
            }
            return out;
        }
    }

    private void bind(PreparedStatement ps, Object[] params) throws SQLException {
        if (params == null) return;
        for (int i = 0; i < params.length; i++) {
            Object v = params[i];
            if (v == null) ps.setNull(i + 1, Types.NULL);
            else if (v instanceof Integer iv) ps.setInt(i + 1, iv);
            else if (v instanceof Long lv) ps.setLong(i + 1, lv);
            else if (v instanceof Double dv) ps.setDouble(i + 1, dv);
            else if (v instanceof String sv) ps.setString(i + 1, sv);
            else ps.setObject(i + 1, v);
        }
    }

    public void close() {
        closed = true;
        if (writerThread != null) writerThread.interrupt();
        try { writeConn.close(); } catch (SQLException ignore) {}
    }
}
