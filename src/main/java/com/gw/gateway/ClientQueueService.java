package com.gw.gateway;

import com.gw.config.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 按接入客户端（zcode/claude-code/…）的同源并发控制：把同一工具的高频连发串行化，
 * 防止它一次砸太多并发请求把免费上游的 TPM/RPM 顶一击打爆（429 风暴的源头）。
 *
 * 设计：
 * - 每个 client 一把公平信号量，许可数 = defaults.clientMaxConcurrency（默认 2，0=关闭）。
 * - 请求在"转发前"acquire：并发 > 许可数的请求会排队，等上一个完整完成（含流式传完）再放行
 *   —— 正是用户要的"上一个完成再下一个"。
 * - 用 tryAcquire + 超时兜底：排队过久（默认等 clientMaxConcurrencyWaitMs）时放行一个去撞墙拿
 *   真实结果，绝不无限阻塞（铁律：排队不能变成请求失败/让下游干等）。
 * - 按 client 维度而非 provider 维度：问题在"一个工具同源连发"，跨工具并发本来就被上游天然错开。
 */
@Service
public class ClientQueueService {

    private static final Logger log = LoggerFactory.getLogger(ClientQueueService.class);

    private final ConfigService configService;
    /** client → 并发控制（含信号量与当前许可数快照，供配置热更重建） */
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    public ClientQueueService(ConfigService configService) {
        this.configService = configService;
    }

    /** 当前 client 允许的最大并发（配置；0/负=不限制） */
    public int maxConcurrency() {
        int c = configService.snapshot().config.getDefaults().getClientMaxConcurrency();
        return c <= 0 ? 0 : c;
    }

    /**
     * 转发前获取同源并发许可。
     * @return 句柄令牌；务必 finally 里 release()。acquired=false 表示未限制或不需排队。
     */
    public QueueToken acquire(String client) {
        int max = maxConcurrency();
        if (max <= 0) return new QueueToken(client, null, false, false);   // 功能关闭
        Entry e = entries.computeIfAbsent(client, k -> new Entry(max));
        // 配置的并发数变化时重建（仅当无人持有，避免丢许可）
        if (e.maxPermits != max) {
            Entry replacement = new Entry(max);
            // 并发场景下非原子，但足够：新请求用新配置，旧请求用旧配置各自 release 到自己的信号量
            entries.replace(client, e, replacement);
            e = entries.get(client);
            if (e == null) e = new Entry(max);
        }
        if (e.tryAcquire()) return new QueueToken(client, e, true, false);

        // 许可耗尽 → 排队等待
        int waitMs = configService.snapshot().config.getDefaults().getClientMaxConcurrencyWaitMs();
        long start = System.nanoTime();
        try {
            boolean ok = e.tryAcquire(waitMs > 0 ? waitMs : 3000, TimeUnit.MILLISECONDS);
            if (ok) return new QueueToken(client, e, true, false);
            log.warn("client={} 排队超时（>{}ms），放行撞墙探测防阻塞", client, waitMs);
            // 超时未拿到许可：acquired=false，release 变成 no-op，绝不放行一个不存在的许可
            // （若误标 acquired=true，release 会 semaphore.release() 凭空多许可，逐步撑爆并发上限）
            return new QueueToken(client, e, false, true);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("client={} 排队被中断，放行", client);
            return new QueueToken(client, e, false, true);
        }
    }

    /** 释放同源许可（一定执行，含异常路径；幂等，重复 release 不会超发许可） */
    public void release(QueueToken token) {
        if (token == null || !token.acquired || token.entry == null) return;
        token.entry.release();
    }

    /** 当前某 client 排队等待数（诊断用） */
    public int queued(String client) {
        Entry e = entries.get(client);
        return e == null ? 0 : e.waiting();
    }

    /** 当前所有已触发的 client 并发状态（诊断用）：许可数 + 排队数 */
    public Map<String, Object> view() {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("enabled", maxConcurrency() > 0);
        out.put("maxConcurrency", maxConcurrency());
        out.put("waitMs", configService.snapshot().config.getDefaults().getClientMaxConcurrencyWaitMs());
        Map<String, Object> clients = new ConcurrentHashMap<>();
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            Map<String, Object> s = new java.util.LinkedHashMap<>();
            s.put("max", e.getValue().maxPermits);
            s.put("waiting", e.getValue().waiting());
            clients.put(e.getKey(), s);
        }
        out.put("clients", clients);
        return out;
    }

    /** 每个 client 的并发控制实现（公平信号量 + 许可数快照） */
    private static final class Entry {
        final int maxPermits;
        private final Semaphore semaphore;

        Entry(int max) {
            this.maxPermits = max;
            this.semaphore = new Semaphore(max, true);   // 公平：先排队先拿，避免饿死
        }

        boolean tryAcquire() { return semaphore.tryAcquire(); }
        boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            return semaphore.tryAcquire(timeout, unit);
        }
        void release() {
            // 幂等守卫：Semaphore 本身无"是否已持有"概念，但允许数已被 tryAcquire 精确配对，
            // 这里仅在合法持有许可时被调用（acquire 成功才 acquired=true）。为防御重复 release
            // 导致许可数超过 maxPermits，用 availablePermits 夹住：只在未逾额时释放空闲许可。
            if (semaphore.availablePermits() < maxPermits) semaphore.release();
        }
        int waiting() { return semaphore.getQueueLength(); }
    }

    /** 排队许可句柄 */
    public static class QueueToken {
        final String client;
        final Entry entry;
        final boolean acquired;      // false=功能关闭未启用
        final boolean timedOut;      // true=排队超时强制放行

        QueueToken(String client, Entry entry, boolean acquired, boolean timedOut) {
            this.client = client;
            this.entry = entry;
            this.acquired = acquired;
            this.timedOut = timedOut;
        }
    }
}