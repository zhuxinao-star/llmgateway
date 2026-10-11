package com.gw.stats;

import jakarta.servlet.http.HttpServletResponse;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 请求链路追踪上下文：一次客户端请求进入网关时生成 traceId，绑定到当前处理线程，
 * 供本次请求全链路的多行 request_log（REJECTED / 多 attempt FAILED / SUCCESS / 流式 /
 * 压缩子请求）共享同一 trace，#31 据 traceId 反查还原整条时间线。
 *
 * <p>为什么用 ThreadLocal 而非逐层显式传参：所有 logRequest 调用点都在 Servlet 请求线程内
 * 同步发起（对冲执行返回后回到本线程才记账，异步落库在 offer 时已把值固化进队列），
 * 且每个入口方法在处理任何日志前都会调用 {@link #start} 覆盖旧值——因此同线程串行请求
 * 天然不会串 trace。观测字段绝不允许影响请求，故 start 内所有异常一律吞掉。
 */
public final class RequestTrace {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private RequestTrace() {
    }

    /**
     * 生成并绑定本次请求 traceId，同时写响应头 X-Gateway-Trace-Id（在首字节前调用，
     * 流式响应也能带上）。返回 traceId；resp 可为 null（如内部再入）。
     */
    public static String start(HttpServletResponse resp) {
        String id = newId();
        CURRENT.set(id);
        if (resp != null) {
            try {
                resp.setHeader("X-Gateway-Trace-Id", id);
            } catch (Exception ignore) {
                // 响应已提交等极端情况：观测失败绝不影响请求
            }
        }
        return id;
    }

    /** 当前线程绑定的 traceId；无则 null（落库 NULL）。 */
    public static String get() {
        return CURRENT.get();
    }

    /** 解绑（供入口 finally 或线程池复用时清理；start 覆盖已足够保证正确性，此处仅卫生）。 */
    public static void clear() {
        CURRENT.remove();
    }

    /** 供压缩等内部子请求临时借用/恢复父 trace（#30 记账沿用父请求 trace）。 */
    public static String peek() {
        return CURRENT.get();
    }

    /** t{epochMillis}-{4位随机}：时间前缀便于按天/时序 grep，随机后缀防同毫秒碰撞。 */
    private static String newId() {
        int r = ThreadLocalRandom.current().nextInt(10000);
        return "t" + System.currentTimeMillis() + "-" + String.format("%04d", r);
    }
}
