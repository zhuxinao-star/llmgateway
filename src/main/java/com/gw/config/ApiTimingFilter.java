package com.gw.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 管理接口计时（排查"点链路很卡"时补的边界证据）：
 * 前端秒表只能测到 net 总时长，分不清"网关算得慢"还是"浏览器到 127.0.0.1 这段路慢"。
 * 这里给每个 /api/** 请求记下服务端自己的耗时，留在内存环形缓冲里，
 * 由 /api/perflog 只读端点回吐，前端的"复制诊断"把两边同一时刻的数字一起贴出来即可对账。
 * 慢请求（&gt;=200ms）额外落一行 WARN 到 gateway.log，事后翻日志也能看到。
 *
 * 只读内存、不写库、不改响应体：对 /api 的其它行为零影响。
 */
@Component
public class ApiTimingFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(ApiTimingFilter.class);
    private static final int CAP = 400;
    private static final long SLOW_MS = 200;

    /** 一次 /api 调用的服务端视角。ts 用 epoch 毫秒，前端 performance.timeOrigin 可直接对齐。 */
    public record Call(long ts, String method, String path, int status, long ms) {}

    private final ArrayDeque<Call> ring = new ArrayDeque<>();
    private long total, slow;

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        String path = req.getRequestURI();
        // perflog 自身不记账：否则诊断请求会挤掉真正要看的那几条
        if (path == null || !path.startsWith("/api/") || path.equals("/api/perflog")) {
            chain.doFilter(req, resp);
            return;
        }
        long startNs = System.nanoTime();
        try {
            chain.doFilter(req, resp);
        } finally {
            long ms = (System.nanoTime() - startNs) / 1_000_000;
            record(new Call(System.currentTimeMillis(), req.getMethod(), path, resp.getStatus(), ms));
            if (ms >= SLOW_MS) {
                log.warn("管理接口慢请求 {} {} -> {}，服务端耗时 {}ms",
                        req.getMethod(), path, resp.getStatus(), ms);
            }
        }
    }

    private void record(Call c) {
        synchronized (ring) {
            total++;
            if (c.ms() >= SLOW_MS) slow++;
            ring.addLast(c);
            while (ring.size() > CAP) ring.removeFirst();
        }
    }

    /** 最近的 /api 调用（时间升序）。since>0 时只返回该时刻之后的记录，前端增量拉取用。 */
    public List<Call> recent(long since, int limit) {
        synchronized (ring) {
            List<Call> all = new ArrayList<>(ring);
            if (since > 0) all = all.stream().filter(c -> c.ts() > since).toList();
            int n = limit <= 0 || limit > all.size() ? all.size() : limit;
            return new ArrayList<>(all.subList(all.size() - n, all.size()));
        }
    }

    public Map counts() {
        synchronized (ring) {
            return Map.of("total", total, "slow", slow, "kept", (long) ring.size(), "slowMs", SLOW_MS);
        }
    }

    /** 当前缓冲窗口的起点（最早一条记录的时刻）；窗口已被挤掉则为 0。 */
    public long windowStart() {
        synchronized (ring) {
            Call first = ring.peekFirst();
            return first == null ? 0 : first.ts();
        }
    }
}
