package com.gw.gateway;

import org.springframework.mock.web.MockHttpServletResponse;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 快速单测（main 直跑）：SseKeepAlive 的三条核心语义。
 *   T1 commit 以 200 text/event-stream 下发响应头（延迟提交，仅在候选真正开始吐流后调用）；
 *   T2 静默满一个间隔即补 : ping（客户端空闲计时持续复位）；
 *   T3 真实写出复位静默计时（刚发过真帧不再多余 ping）。
 * 注：开局前硬失败改回真实 HTTP 429/503（不再降级为流内 error），故不再测流内收尾帧。
 */
public class SseKeepAliveTest {

    static int failures = 0;

    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "PASS " : "FAIL ") + name + (cond ? "" : "  ← " + detail));
        if (!cond) failures++;
    }

    static int count(String s, String sub) {
        int n = 0, i = 0;
        while ((i = s.indexOf(sub, i)) >= 0) { n++; i += sub.length(); }
        return n;
    }

    static String body(MockHttpServletResponse r) {
        return new String(r.getContentAsByteArray(), StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws Exception {
        // ---- T1 commit 下发头 ----
        {
            MockHttpServletResponse resp = new MockHttpServletResponse();
            SseKeepAlive ka = new SseKeepAlive(resp, 150);
            ka.commit();
            check("T1 状态 200", resp.getStatus() == 200, "got " + resp.getStatus());
            check("T1 Content-Type=text/event-stream",
                    String.valueOf(resp.getContentType()).startsWith("text/event-stream"), String.valueOf(resp.getContentType()));
            check("T1 X-Accel-Buffering=no", "no".equals(resp.getHeader("X-Accel-Buffering")),
                    String.valueOf(resp.getHeader("X-Accel-Buffering")));
            check("T1 Cache-Control=no-cache", "no-cache".equals(resp.getHeader("Cache-Control")),
                    String.valueOf(resp.getHeader("Cache-Control")));
            ka.stop();
        }

        // ---- T2 静默 → 补 ping ----
        {
            MockHttpServletResponse resp = new MockHttpServletResponse();
            SseKeepAlive ka = new SseKeepAlive(resp, 120);
            ka.commit();
            ka.start();
            Thread.sleep(560);   // ~4+7 个间隔，无任何真实写出
            ka.stop();
            Thread.sleep(120);   // 等在途 ticker 退出
            int pings = count(body(resp), ": ping\n\n");
            check("T2 空闲期补发 >=2 个 : ping 帧", pings >= 2, "pings=" + pings + " body=" + body(resp));
        }

        // ---- T3 真实写出复位空闲计时 → 不该 ping ----
        {
            MockHttpServletResponse resp = new MockHttpServletResponse();
            SseKeepAlive ka = new SseKeepAlive(resp, 400);   // 间隔远大于写入节奏
            ka.commit();
            ka.start();
            OutputStream out = ka.guarded();
            long t0 = System.currentTimeMillis();
            int frames = 0;
            while (System.currentTimeMillis() - t0 < 1000) {   // 每 ~100ms 一帧，静默始终 < 400ms
                out.write(("data: f" + (frames++) + "\n\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
                Thread.sleep(100);
            }
            ka.stop();
            Thread.sleep(150);
            String b = body(resp);
            int pings = count(b, ": ping\n\n");
            check("T3 持续真写出时不补 ping（复位空闲）", pings == 0, "pings=" + pings + " frames=" + frames);
            check("T3 真实帧照常透传", count(b, "data: f") == frames, "have=" + count(b, "data: f") + " want=" + frames);
        }

        System.out.println("\n=== " + (failures == 0 ? "ALL PASS" : failures + " FAILURE(S)") + " ===");
        if (failures > 0) System.exit(1);
    }
}
