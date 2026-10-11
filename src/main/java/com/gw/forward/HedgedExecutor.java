package com.gw.forward;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.policy.PolicyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 对冲请求执行器（Hedged Requests）：响应速度优化。
 *
 * 策略：主选候选先发（不加任何延迟）；主选在 hedgeDelayMs 窗口内拿到首字/失败则一切如常；
 * 迟迟不出首字时并发次选候选竞速——谁先出非 FAILED 结果谁胜，输家立刻取消
 * （上游连接掐断，几乎不产 token）。双败回退 attempt 循环换下一家。
 *
 * 常态（主选健康）零额外消耗；慢渠道场景秒级切换，用户无感。
 */
@Service
public class HedgedExecutor {

    private static final Logger log = LoggerFactory.getLogger(HedgedExecutor.class);

    private final ConfigService configService;
    private final ForwardEngine forwardEngine;
    /** 竞速线程池：每任务一个虚拟线程（虚拟线程默认即 daemon），无固定线程数无泄漏 */
    private final ExecutorService racer = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("gw-hedge-", 0).factory());

    public HedgedExecutor(ConfigService configService, ForwardEngine forwardEngine) {
        this.configService = configService;
        this.forwardEngine = forwardEngine;
    }

    /**
     * 执行一次（可能带对冲的）转发。
     * @param openAiRequest OpenAI 格式请求体（各 racer 独立浅拷贝）
     * @param ranked 已排序候选（rank 顺序）
     * @param primaryIndex 本轮的主选下标
     * @param stream 是否流式
     * @param deadlineNanos 总预算截止
     * @return 胜者结果（STREAM 含首行）；FAILED = 主选+对冲都失败
     */
    public ForwardEngine.ForwardResult execute(Map<String, Object> openAiRequest,
                                               List<PolicyService.Scored> ranked,
                                               int primaryIndex, boolean stream,
                                               long deadlineNanos) {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        PolicyService.Candidate primary = ranked.get(primaryIndex).candidate;
        PolicyService.Scored hedge = hedgeCandidate(ranked, primaryIndex);

        // 大上下文请求禁用对冲：152K 请求双发 = 两倍 TPM 消耗（sensenova 免费配额本来
        // 就小），且大请求都慢（预填充为主），对冲翻不出速度收益，纯串行 failover 更省
        int estTokens = com.gw.policy.RequestCategoryAnalyzer.estimateInputTokens(openAiRequest);
        boolean largeContext = estTokens > d.getHedgeMaxContextTokens();

        // 无对冲对象/开关关闭/非流式/大请求：退化为普通单发（保持原行为）
        if (!stream || hedge == null || d.getHedgeDelayMs() <= 0 || largeContext) {
            return forwardEngine.execute(openAiRequest, primary, stream, deadlineNanos);
        }
        // 对冲窗口不超过 TTFT 预算的一半（给竞速留时间）
        long hedgeDelayMs = Math.min(d.getHedgeDelayMs(), d.getTtftTimeoutMs() / 2);

        // ---- 主选先发 ----
        long t0 = System.nanoTime();
        Future<ForwardEngine.ForwardResult> primaryF = racer.submit(() ->
                forwardEngine.execute(copyOf(openAiRequest), primary, true, deadlineNanos));

        ForwardEngine.ForwardResult primaryR = poll(primaryF, hedgeDelayMs);

        // 主选窗口内完成：成功直接用；快速失败交 attempt 循环换家（不再对冲，避免放大故障）
        if (primaryR != null) {
            if (primaryR.kind == ForwardEngine.ForwardResult.Kind.FAILED && primaryR.timeout) {
                log.info("主选 {}/{} 首字超时，attempt 循环切换下一家", primary.providerId(), primary.model());
            }
            return primaryR;
        }

        // ---- 主选慢：并发次选竞速 ----
        log.info("主选 {}/{} {}ms 未出首字，发起对冲竞速 → {}/{}",
                primary.providerId(), primary.model(), hedgeDelayMs,
                hedge.candidate.providerId(), hedge.candidate.model());
        Future<ForwardEngine.ForwardResult> hedgeF = racer.submit(() ->
                forwardEngine.execute(copyOf(openAiRequest), hedge.candidate, true, deadlineNanos));

        long raceDeadlineMs = System.currentTimeMillis() +
                TimeUnit.NANOSECONDS.toMillis(Math.max(1, deadlineNanos - System.nanoTime()));
        while (true) {
            long remainMs = raceDeadlineMs - System.currentTimeMillis();
            if (remainMs <= 0) {
                // 竞速超总预算：两个都掐，回退主选结果（若已完成）或双败
                ForwardEngine.ForwardResult rp = cancelAndGet(primaryF);
                ForwardEngine.ForwardResult rh = cancelAndGet(hedgeF);
                return rp != null ? rp : (rh != null ? rh : fail("对冲竞速超预算"));
            }
            ForwardEngine.ForwardResult rp = poll(primaryF, Math.min(remainMs, 150));
            if (rp != null && rp.kind != ForwardEngine.ForwardResult.Kind.FAILED) {
                cancelQuietly(hedgeF);
                log.info("对冲竞速：主选胜出 {}/{}（{}ms）", rp.providerId, rp.model,
                        (System.nanoTime() - t0) / 1_000_000);
                return rp;
            }
            ForwardEngine.ForwardResult rh = poll(hedgeF, Math.min(Math.max(1, remainMs - 150), 150));
            if (rh != null && rh.kind != ForwardEngine.ForwardResult.Kind.FAILED) {
                cancelQuietly(primaryF);
                log.info("对冲竞速：备选胜出 {}/{}（{}ms）", rh.providerId, rh.model,
                        (System.nanoTime() - t0) / 1_000_000);
                return rh;
            }
            // 两个都完成但都 FAILED → 双败（取主选错误，attempt 循环继续）
            if (isSettled(primaryF) && isSettled(hedgeF)) {
                ForwardEngine.ForwardResult a = resultOf(primaryF), b = resultOf(hedgeF);
                return a != null ? a : (b != null ? b : fail("对冲双败"));
            }
        }
    }

    // ================= 工具 =================

    private ForwardEngine.ForwardResult poll(Future<ForwardEngine.ForwardResult> f, long ms) {
        try {
            return f.get(ms, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /** 已完成则取结果并取消读流（掐断连接）；未完成/已取消返回 null */
    private ForwardEngine.ForwardResult cancelAndGet(Future<ForwardEngine.ForwardResult> f) {
        if (f.isDone() && !f.isCancelled()) {
            try {
                ForwardEngine.ForwardResult r = f.get();
                if (r != null && r.kind == ForwardEngine.ForwardResult.Kind.STREAM && r.streamReader != null) {
                    try { r.streamReader.close(); } catch (Exception ignore) {}
                }
                return r;
            } catch (Exception e) {
                return null;
            }
        }
        cancelQuietly(f);
        return null;
    }

    private void cancelQuietly(Future<?> f) {
        if (f == null) return;
        // 输家已完成且返回了 STREAM（读到首字但竞速输了）：f.cancel(true) 对它无效，
        // 必须显式 close reader 掐断上游连接，否则该连接会占位到 HTTP 超时（30s）才释放。
        if (f.isDone() && !f.isCancelled()) {
            try {
                Object r = f.get();
                if (r instanceof ForwardEngine.ForwardResult fr
                        && fr.kind == ForwardEngine.ForwardResult.Kind.STREAM && fr.streamReader != null) {
                    try { fr.streamReader.close(); } catch (Exception ignore) {}
                }
                return;
            } catch (Exception ignore) { return; }
        }
        // 未完成：cancel(true) 打断其在 TTFT 轮询里的 sleep，由 execute 内部 closeQuietly 清理连接
        f.cancel(true);
    }

    private boolean isSettled(Future<ForwardEngine.ForwardResult> f) {
        // 已取消视为完成（不会再有结果）
        return f.isDone();
    }

    private ForwardEngine.ForwardResult resultOf(Future<ForwardEngine.ForwardResult> f) {
        try {
            return f.isDone() && !f.isCancelled() ? f.get() : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** 对冲候选：主选之后第一个不同 (provider,model) 且非 blocked 的候选。
     *  跳过非 free 档——对冲是"竞速陪跑"，输家被掐断但首字已产生消耗；
     *  付费模型不该当陪跑（付费只做穷尽后的兜底，哲学与评分 C 因子一致）。
     *  主选自己是付费（latency 策略等场景）时允许对冲同档付费，避免退化单发。 */
    private PolicyService.Scored hedgeCandidate(List<PolicyService.Scored> ranked, int primaryIndex) {
        PolicyService.Scored primary = ranked.get(primaryIndex);
        String primaryTier = configService.snapshot().effectiveCostTier(
                primary.candidate.providerId(), primary.candidate.model());
        boolean primaryFree = "free".equalsIgnoreCase(primaryTier);
        for (int i = primaryIndex + 1; i < ranked.size(); i++) {
            PolicyService.Scored s = ranked.get(i);
            if (s.blocked) continue;
            if (s.candidate.providerId().equals(primary.candidate.providerId())
                    && s.candidate.model().equals(primary.candidate.model())) continue;  // 同模型多 Key 不对冲
            if (primaryFree) {
                String tier = configService.snapshot().effectiveCostTier(
                        s.candidate.providerId(), s.candidate.model());
                if (!"free".equalsIgnoreCase(tier)) continue;  // 免费主选的对冲陪跑也必须免费
            }
            return s;
        }
        return null;
    }

    private ForwardEngine.ForwardResult fail(String err) {
        return new ForwardEngine.ForwardResult(ForwardEngine.ForwardResult.Kind.FAILED, 0, err, null,
                null, null, null, null, null, null, 0, err, false, -1);
    }

    private static Map<String, Object> copyOf(Map<String, Object> src) {
        return new LinkedHashMap<>(src);
    }
}
