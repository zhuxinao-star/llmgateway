package com.gw.gateway;

import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 下游 SSE 保活：在向客户端透传流式响应的整个生命周期里，凡是"客户端在等、但网关没字节可发"
 * 的静默窗口——同源排队、上游响应头/TTFT 等待、两个 token 之间的生成空档——都按固定间隔补发
 * 一行 SSE 注释帧 {@code : ping\n\n}。注释帧被 OpenAI/Anthropic SDK 解析器忽略，却能持续复位
 * 工具端的读空闲计时器，从根上解决"模型慢→工具莫名断开"。
 *
 * <p>用法（仅 stream=true 且 intervalMs>0）：候选**真正开始吐流时**（execute 返回 STREAM）才调
 * {@link #commit()} 把响应头以 200 text/event-stream 下发（此后状态码/响应头不可再改），
 * {@link #start()} 拉起后台 ticker，写循环拿 {@link #guarded()} 的输出流写真实帧，
 * finally 里 {@link #stop()} 停 ticker。延迟提交是关键：开局前（排队/响应头/TTFT）的硬失败
 * 仍回真实 HTTP 429/503，让工具按退避重试——提前 commit 会把可重试错误降级成"200+流内 error"，
 * 而 Claude Code 对流内 error 不重试。intervalMs<=0 时调用方走原路径（不 commit、不保活）。
 */
public final class SseKeepAlive {

    private static final byte[] PING = ": ping\n\n".getBytes(StandardCharsets.UTF_8);

    private final HttpServletResponse response;
    private final long intervalMs;
    private final Object lock = new Object();
    private OutputStream raw;                 // commit() 后有效
    private volatile long lastWriteAt;        // 最近一次真实写出（含 ping）的时刻
    private volatile boolean running;
    private Thread ticker;

    public SseKeepAlive(HttpServletResponse response, long intervalMs) {
        this.response = response;
        this.intervalMs = intervalMs;
    }

    /** 提交 SSE 响应头并拿到输出流，立即 flush 让客户端收到 200 首字节；此后不可再改状态码/头。 */
    public void commit() throws IOException {
        response.setStatus(200);
        response.setContentType("text/event-stream");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("X-Accel-Buffering", "no");
        raw = response.getOutputStream();
        raw.flush();
        lastWriteAt = System.currentTimeMillis();
    }

    /** 拉起后台虚拟线程 ticker：静默满一个间隔即向客户端补一行 ping。 */
    public void start() {
        running = true;
        ticker = Thread.ofVirtual().name("gw-sse-keepalive").start(this::loop);
    }

    private void loop() {
        long step = Math.max(200, Math.min(intervalMs, 1000));
        while (running) {
            try {
                Thread.sleep(step);
            } catch (InterruptedException e) {
                return;
            }
            if (!running) return;
            if (System.currentTimeMillis() - lastWriteAt >= intervalMs) {
                try {
                    pingNow();
                } catch (IOException e) {
                    // 客户端已断开：写出必失败，静默停掉 ticker（真实断开由写循环记账）
                    return;
                }
            }
        }
    }

    private void pingNow() throws IOException {
        synchronized (lock) {
            if (!running) return;
            raw.write(PING);
            raw.flush();
            lastWriteAt = System.currentTimeMillis();
        }
    }

    /**
     * 包一层写出流交绐透传循环：每次 write 复位静默计时（刚发过真帧就不必再 ping），
     * 与 ping 共用同一把锁——因每帧都以 \n\n 自终止且单次 write 原子，ping 只会落在帧之间。
     */
    public OutputStream guarded() {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                synchronized (lock) { raw.write(b); lastWriteAt = System.currentTimeMillis(); }
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                synchronized (lock) { raw.write(b, off, len); lastWriteAt = System.currentTimeMillis(); }
            }

            @Override
            public void flush() throws IOException {
                synchronized (lock) { raw.flush(); }
            }

            @Override
            public void close() throws IOException {
                synchronized (lock) { raw.flush(); }   // 不关底层 servlet 流，交给容器收尾
            }
        };
    }

    /** 停 ticker（幂等）；在请求 finally 里调用，确保不再向已收尾的响应写 ping。 */
    public void stop() {
        running = false;
        Thread t = ticker;
        if (t != null) t.interrupt();
    }
}
