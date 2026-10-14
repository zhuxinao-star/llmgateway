package com.gw.stats;

import com.gw.policy.PolicyService;
import com.gw.store.StatsDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

/**
 * 统计模块：请求日志 → 内存队列 → 异步落库；
 * runtime_snapshot 定期持久化（60s）+ 旧日志清理（30 天）。
 * 只发事件不改状态（状态变更走 policy）。
 */
@Service
public class StatsService {

    private static final Logger log = LoggerFactory.getLogger(StatsService.class);
    /** 专用请求日志 logger：application.yaml 中 REQUEST_LOG_FILE 单独落 gateway.log */
    private static final Logger requestLog = LoggerFactory.getLogger("REQUEST");

    private final StatsDao dao;
    private final PolicyService policyService;
    private final BlockingQueue<Object[]> logQueue = new LinkedBlockingQueue<>(50_000);

    public StatsService(StatsDao dao, PolicyService policyService) {
        this.dao = dao;
        this.policyService = policyService;
        Thread.ofVirtual().name("gw-stats-writer").start(this::writeLoop);
        Thread.ofVirtual().name("gw-snapshot-writer").start(this::snapshotLoop);
        Thread.ofVirtual().name("gw-log-cleaner").start(this::cleanLoop);
    }

    /** 请求日志事件（异步落 SQLite + 同步写 gateway.log）——无压缩诊断的调用点走这里，compress 记 null */
    public void logRequest(String modelReq, String providerId, String modelReal,
                           Integer ttftMs, Integer totalMs, Long tokensIn, Long tokensOut,
                           String status, int attempts, String profile, String error, String client) {
        logRequest(modelReq, providerId, modelReal, ttftMs, totalMs, tokensIn, tokensOut,
                status, attempts, profile, error, client, null);
    }

    /** 请求日志事件——带压缩诊断 tag（增量/全量/硬截断/未触发），无链路（trace=null） */
    public void logRequest(String modelReq, String providerId, String modelReal,
                           Integer ttftMs, Integer totalMs, Long tokensIn, Long tokensOut,
                           String status, int attempts, String profile, String error, String client,
                           String compress) {
        logRequest(modelReq, providerId, modelReal, ttftMs, totalMs, tokensIn, tokensOut,
                status, attempts, profile, error, client, compress, null, null, null, null);
    }

    /** 完整签名：追加 traceId（同一次客户端请求串联多 attempt 行）、httpStatus、errorClass、channelId。
     *  四个观测字段任一为空即落 NULL，绝不因观测缺失影响记录（CLIENT_ABORTED 同哲学）。
     *  channelId = 这一发实际用的那把 Key（供应商内渠道 id）；一家多把 Key 时靠它做归属与统计。 */
    public void logRequest(String modelReq, String providerId, String modelReal,
                           Integer ttftMs, Integer totalMs, Long tokensIn, Long tokensOut,
                           String status, int attempts, String profile, String error, String client,
                           String compress, String traceId, Integer httpStatus, String errorClass,
                           String channelId) {
        // traceId 未显式传入则继承当前请求线程的链路上下文（网关入口 RequestTrace.start 绑定）；
        // 无上下文（如后台探测）则落 NULL。观测缺失绝不影响记录。
        if (traceId == null) traceId = RequestTrace.get();
        String cl = client == null || client.isBlank() ? "unknown" : client;
        // errorClass 未显式传入则按 (httpStatus, error) 语义派生：与 recordFailure 同一分类器，
        // 保证"落库的分类"与"路由据以冷却的分类"永远一致（单一事实源）。OK 行不派生（留 NULL）。
        String ec = errorClass;
        if (ec == null && error != null && !error.isBlank())
            ec = com.gw.policy.UpstreamErrorClassifier.classify(httpStatus, error).name();
        logQueue.offer(new Object[]{System.currentTimeMillis(), modelReq, providerId, modelReal,
                ttftMs, totalMs, tokensIn, tokensOut, status, attempts, profile, error, cl, compress,
                traceId, httpStatus, ec, channelId});
        // 每次请求都写 gateway.log（含失败尝试）：一行一条，便于 grep 排查；压缩诊断同列
        requestLog.info("req client={} model={} provider={}/{} key={} ttft={}ms total={}ms tokens={}+{} status={} attempts={} profile={} err={} compress={} trace={} http={} class={}",
                cl,
                modelReq, providerId == null ? "-" : providerId, modelReal == null ? "-" : modelReal,
                channelId == null || channelId.isBlank() ? "-" : channelId,
                ttftMs == null ? "-" : ttftMs, totalMs == null ? "-" : totalMs,
                tokensIn == null ? 0 : tokensIn, tokensOut == null ? 0 : tokensOut,
                status, attempts, profile == null ? "-" : profile,
                error == null ? "-" : error.replaceAll("[\\r\\n]", " "),
                compress == null ? "-" : compress,
                traceId == null ? "-" : traceId,
                httpStatus == null ? "-" : httpStatus,
                errorClass == null ? "-" : errorClass);
    }

    private void writeLoop() {
        List<Object[]> batch = new ArrayList<>();
        while (true) {
            try {
                Object[] first = logQueue.poll(1, TimeUnit.SECONDS);
                if (first == null) continue;
                batch.clear();
                batch.add(first);
                logQueue.drainTo(batch, 199);
                for (Object[] r : batch) {
                    dao.insertRequestLog((Long) r[0], (String) r[1], (String) r[2], (String) r[3],
                            (Integer) r[4], (Integer) r[5], (Long) r[6], (Long) r[7],
                            (String) r[8], (Integer) r[9], (String) r[10], (String) r[11], (String) r[12],
                            (String) r[13], (String) r[14], (Integer) r[15], (String) r[16], (String) r[17]);
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                log.error("请求日志批量写失败: {}", e.getMessage());
            }
        }
    }

    private void snapshotLoop() {
        while (true) {
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                return;
            }
            // 两件事各自一个 try：学习层读库失败不能连累运行态快照落盘，反之亦然。
            // 此前共用一个 try，一次 NPE 就让 persistSnapshot 连续 28 分钟没跑（2026-10-14）。
            try {
                policyService.refreshChronicRateLimits();   // 慢性限流读库刷新（重启后第一发就能认出长期撞墙的家）
            } catch (Exception e) {
                log.warn("慢性限流读库刷新失败: {}", describe(e));
            }
            try {
                policyService.persistSnapshot();
            } catch (Exception e) {
                log.warn("运行时快照保存失败: {}", describe(e));
            }
        }
    }

    /** 只打 e.getMessage() 会把 NPE 打成 "失败: null"，人无法诊断——带上异常类名与首条栈帧 */
    private static String describe(Exception e) {
        StackTraceElement at = e.getStackTrace().length == 0 ? null : e.getStackTrace()[0];
        return e.getClass().getSimpleName() + ": " + e.getMessage() + (at == null ? "" : " @ " + at);
    }

    private void cleanLoop() {
        while (true) {
            try {
                Thread.sleep(6 * 3600_000); // 每 6 小时清一次
                dao.cleanupOldLogs(30);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /** 管理页查询：当日用量 */
    public List<Map<String, Object>> todayUsageView() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (StatsDao.UsageRow r : dao.loadUsageOfDay(policyService.today())) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("provider", r.providerId());
            row.put("model", r.model());
            row.put("requests", r.reqCount());
            row.put("tokensIn", r.tokensIn());
            row.put("tokensOut", r.tokensOut());
            out.add(row);
        }
        return out;
    }

    /** 管理页查询：按接入工具聚合用量（日历日区间 [from,to]，与用量主窗口同口径+同过滤） */
    public List<Map<String, Object>> clientUsageView(String from, String to,
                                                     List<String> providers, List<String> models, List<String> clients) {
        List<StatsDao.ClientUsageRow> rs = dao.usageByClient(from, to, providers, models, clients);
        List<Map<String, Object>> out = new ArrayList<>();
        for (StatsDao.ClientUsageRow r : rs) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("client", r.client());
            row.put("requests", r.reqCount());
            row.put("tokensIn", r.tokensIn());
            row.put("tokensOut", r.tokensOut());
            row.put("tokens", r.tokensIn() + r.tokensOut());
            out.add(row);
        }
        return out;
    }
}
