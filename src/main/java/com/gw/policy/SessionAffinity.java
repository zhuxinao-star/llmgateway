package com.gw.policy;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.provider.adaptor.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话粘性路由：同一会话内钉住上次成功的候选，让长 Agent 任务的每一轮由同一颗模型作答。
 *
 * 动机（2026-11-17 request_log 实测）：一个 115 轮的 zcode 会话走 free-fast 组，
 * 被 8+ 家模型轮流作答（deepseek-v4-flash 47 轮、stepaudio 29 轮、v4-pro 10 轮…），
 * 28 轮首选失败强制换家。每换一次"脑"，新模型把历史重读一遍、用自己的话把已宣布的
 * 结论重新宣布一遍——"找到问题了/找到漏洞了"刷屏的直接推手。
 *
 * 与既有机制的关系：rank 的分摊抽签是"会话间防赢家通吃"（刻意轮换）；粘性是
 * "会话内稳定"的例外——会话间照常分摊，同一会话不换脑。换脑次数只剩两类：
 * 钉住的候选真的失败/冷却（rank 后不进名单，天然解钉），或连续失败 2 次主动解钉。
 *
 * 规则：
 * - 钉住的前提是该候选仍在本次 rank 的可用名单里（冷却/熔断候选不进名单 → 不会钉错）
 * - 同候选连续失败 ≥ {@value #MAX_CONSEC_FAILS} 次 → 停止钉住，下次成功重新钉
 * - 键 = client + 会话指纹（首条 system + 前两条 user 内容哈希）+ 请求模型；
 *   指纹必须取自【压缩前】的原始 messages（压缩会改写 messages，取原文才跨压缩周期稳定）
 * - 条目 30 分钟未用过期（会话暂停后回来不强行复活旧钉）；总量超限删最旧
 */
@Service
public class SessionAffinity {

    private static final Logger log = LoggerFactory.getLogger(SessionAffinity.class);

    /** 同一候选连续失败这么多次后停止钉住（给它一次重试机会，再败换人） */
    private static final int MAX_CONSEC_FAILS = 2;
    /** 条目闲置过期：会话暂停超过这段时间就不再复活旧钉 */
    private static final long TTL_MS = 30 * 60_000L;
    /** 容量兜底：超限删最旧（单机并发会话数量级，正常到不了） */
    private static final int MAX_ENTRIES = 512;

    private static class Affinity {
        final String providerId;
        final String model;
        final String channelId;
        int consecFails = 0;
        long lastUsedAt;
        Affinity(String providerId, String model, String channelId) {
            this.providerId = providerId; this.model = model; this.channelId = channelId;
        }
        boolean matches(PolicyService.Candidate c) {
            return providerId.equals(c.providerId()) && model.equals(c.model());
        }
    }

    private final Map<String, Affinity> byKey = new ConcurrentHashMap<>();
    private final ConfigService configService;

    public SessionAffinity(ConfigService configService) {
        this.configService = configService;
    }

    /**
     * 会话键 = client + 会话指纹 + 请求模型。
     * 指纹取首条 system 内容 + 前两条 user 内容——会话内稳定、跨会话可分；
     * 必须在上下文压缩【之前】对原始 messages 算（压缩改写 messages 后指纹会漂）。
     */
    public String keyOf(Map<String, Object> req, String client, String model) {
        String cl = client == null || client.isBlank() ? "unknown" : client;
        return cl + "#" + sessionFingerprint(Json.asList(req.get("messages"))) + "#" + model;
    }

    /**
     * 会话指纹：首条 system + 前两条 user 的内容哈希（无 system 无 user 时退化 "na"）。
     * 与 ContextCompressor 的滚动缓存分桶共用同一套指纹语义。
     */
    public static String sessionFingerprint(List<Object> messages) {
        java.security.MessageDigest md;
        try {
            md = java.security.MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            return "na";
        }
        StringBuilder seed = new StringBuilder();
        boolean gotSystem = false;
        int users = 0;
        if (messages != null) {
            for (Object o : messages) {
                Map<String, Object> m = Json.asMap(o);
                if (m == null) continue;
                String role = Json.str(m.get("role"));
                Object c = m.get("content");
                String text = c == null ? "" : (c instanceof String s ? s : Json.write(c));
                if ("system".equals(role)) {
                    if (!gotSystem) { gotSystem = true; seed.append("S:").append(text).append('\n'); }
                } else if ("user".equals(role)) {
                    if (users < 2) { users++; seed.append("U").append(users).append(':').append(text).append('\n'); }
                }
                if (gotSystem && users >= 2) break;
            }
        }
        if (seed.length() == 0) return "na";
        md.update(seed.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        byte[] dig = md.digest();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8 && i < dig.length; i++) sb.append(String.format("%02x", dig[i]));
        return sb.toString();
    }

    /**
     * 粘性生效：钉住的候选若在本次 rank 结果里（同 provider+model，优先同渠道），
     * 挪到队首——attempt 循环从队首开始试，其余候选顺序保持不变（failover 链完好）。
     * 功能关闭 / 无钉 / 钉住者连续失败过多 / 钉住者不在名单（冷却中）→ 原样返回。
     */
    public List<PolicyService.Scored> apply(String key, List<PolicyService.Scored> ranked) {
        if (ranked == null || ranked.size() <= 1 || !enabled()) return ranked;
        Affinity aff = byKey.get(key);
        if (aff == null || aff.consecFails >= MAX_CONSEC_FAILS) return ranked;
        int idx = -1;
        for (int i = 0; i < ranked.size(); i++) {
            PolicyService.Candidate c = ranked.get(i).candidate;
            if (aff.matches(c)) { idx = i; if (c.channelId().equals(aff.channelId)) break; }
        }
        if (idx <= 0) return ranked;   // 不在名单（冷却）或已在队首
        List<PolicyService.Scored> out = new ArrayList<>(ranked);
        PolicyService.Scored pinned = out.remove(idx);
        out.add(0, pinned);
        log.debug("[粘性] 会话 {} 钉住 {}/{} 提到队首（原位 {}）", key, aff.providerId, aff.model, idx);
        return out;
    }

    /** 成功（含流式胜者）：钉住该候选，清零连败计数。换了脑子（与上次钉住的不同模型）留痕——
     *  "每次请求不知道前面干了什么/重复干活"排查时，这是第一现场证据。 */
    public void recordSuccess(String key, PolicyService.Candidate c) {
        if (key == null || c == null || !enabled()) return;
        Affinity prev = byKey.put(key, newAffinity(c));
        if (prev != null && !(prev.providerId.equals(c.providerId()) && prev.model.equals(c.model()))) {
            log.info("[粘性] 会话 {} 换脑 {}/{} → {}/{}（上次连败 {} 次）",
                    key, prev.providerId, prev.model, c.providerId(), c.model(), prev.consecFails);
        }
        evictIfNeeded();
    }

    private static Affinity newAffinity(PolicyService.Candidate c) {
        Affinity a = new Affinity(c.providerId(), c.model(), c.channelId());
        a.lastUsedAt = System.currentTimeMillis();
        return a;
    }

    /** 失败：钉住者本人失败才计连败；连败到上限后本键不再生效（下次成功重新钉）。 */
    public void recordFailure(String key, PolicyService.Candidate c) {
        if (key == null || c == null) return;
        Affinity aff = byKey.get(key);
        if (aff == null || !aff.matches(c)) return;
        aff.consecFails++;
        if (aff.consecFails >= MAX_CONSEC_FAILS) {
            log.info("[粘性] 会话 {} 钉住的 {}/{} 连续失败 {} 次，解除钉住", key, aff.providerId, aff.model, aff.consecFails);
            byKey.remove(key);
        }
    }

    private boolean enabled() {
        try {
            GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
            return d.isSessionAffinityEnabled();
        } catch (Exception e) {
            return true;   // 配置读取异常按默认开启（粘性只是排序偏好，不产生正确性风险）
        }
    }

    private void evictIfNeeded() {
        if (byKey.size() <= MAX_ENTRIES) return;
        String oldest = null; long oldestAt = Long.MAX_VALUE;
        for (Map.Entry<String, Affinity> e : byKey.entrySet()) {
            if (e.getValue().lastUsedAt < oldestAt) { oldestAt = e.getValue().lastUsedAt; oldest = e.getKey(); }
        }
        if (oldest != null) byKey.remove(oldest);
        // 顺手清过期项
        long now = System.currentTimeMillis();
        byKey.values().removeIf(a -> now - a.lastUsedAt > TTL_MS);
    }
}
