package com.gw.catalog;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.provider.adaptor.Json;
import com.gw.util.ProviderClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 模型目录：后台定时拉取各家 /v1/models（anthropic 用 /v1/models），
 * 聚合成 model → Set<providerId> 索引；不支持的走静态清单。
 * 目录变更 → 通知 router 重建索引。
 */
@Service
public class CatalogService {

    private static final Logger log = LoggerFactory.getLogger(CatalogService.class);

    private final ConfigService configService;
    private final ProviderClients providerClients;

    /** 目录/余额拉取的连接超时：海外源站直连握手可能慢，沿用原 10s，与转发的 connectTimeoutMs 分开 */
    private static final int CONNECT_TIMEOUT_MS = 10_000;

    /** model(小写) → [providerId]，聚合目录 */
    private volatile Map<String, Set<String>> catalog = Map.of();
    /** providerId → 发现时间戳与 stale 标记 */
    private final Map<String, Long> lastFetchMs = new ConcurrentHashMap<>();
    private volatile Map<String, Boolean> stale = Map.of();

    public CatalogService(ConfigService configService, ProviderClients providerClients) {
        this.configService = configService;
        this.providerClients = providerClients;
    }

    /** 启动后异步首轮 + 定时刷新 */
    public void start() {
        Thread.ofVirtual().name("gw-catalog").start(() -> {
            refreshAll();
            long interval = configService.snapshot().config.getDefaults().getCatalogRefreshMinutes() * 60_000L;
            while (true) {
                try {
                    Thread.sleep(interval);
                    refreshAll();
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    public void refreshAll() {
        GatewayConfig cfg = configService.snapshot().config;
        Map<String, Set<String>> next = new ConcurrentHashMap<>();
        Map<String, Boolean> nextStale = new ConcurrentHashMap<>();
        int fetchTimeout = cfg.getDefaults().getCatalogFetchTimeoutMs();
        boolean discovery = cfg.getDefaults().isCatalogDiscovery();

        for (GatewayConfig.Provider p : cfg.getProviders()) {
            if (!p.isEnabled()) continue;
            Set<String> models = new HashSet<>();
            // 1) 静态清单先入（手工配置的唯一权威来源；发现开关关闭时仅此）
            models.addAll(p.getStaticModels());
            // 2) 动态发现（仅开关开启时）
            if (discovery) {
                try {
                    models.addAll(fetchProviderModels(p, fetchTimeout));
                    lastFetchMs.put(p.getId(), System.currentTimeMillis());
                    nextStale.put(p.getId(), false);
                } catch (Exception e) {
                    log.warn("拉取 {} /v1/models 失败（沿用静态清单/旧目录）: {}", p.getId(), e.getMessage());
                    nextStale.put(p.getId(), true);
                    // 沿用旧目录里该家的模型
                    Set<String> oldModels = new HashSet<>();
                    // catalog 是 model→providers 反向索引，扫一遍恢复该家
                    for (Map.Entry<String, Set<String>> en : catalog.entrySet()) {
                        if (en.getValue().contains(p.getId())) oldModels.add(en.getKey());
                    }
                    models.addAll(oldModels);
                }
            }
            for (String m : models) {
                next.computeIfAbsent(m.toLowerCase(), k -> ConcurrentHashMap.newKeySet()).add(p.getId());
            }
        }
        this.catalog = Map.copyOf(next);
        this.stale = nextStale;
        log.info("模型目录已刷新：{} 个模型 / {} 家提供商（发现开关={}）",
                catalog.size(), cfg.getProviders().size(), discovery ? "on" : "off");
    }

    /** 拉取结果（失败时带状态码，管理页区分限流/其他错误） */
    public record FetchResult(int status, String body) {}
    /** 拉取失败：携带 HTTP 状态码（429=限流等） */
    public static class FetchException extends RuntimeException {
        public final FetchResult result;
        public FetchException(FetchResult result, String message) {
            super(message);
            this.result = result;
        }
    }

    private List<String> fetchProviderModels(GatewayConfig.Provider p, int timeoutMs) throws Exception {
        String url = p.getBaseUrl().replaceAll("/+$", "") + "/models";
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .GET();
        // Anthropic 用 x-api-key + anthropic-version
        if ("anthropic".equals(p.getProtocol())) {
            GatewayConfig.Channel ch = firstEnabledChannel(p);
            if (ch == null) return List.of();
            rb.header("x-api-key", ch.getApiKey()).header("anthropic-version", "2023-06-01");
        } else {
            GatewayConfig.Channel ch = firstEnabledChannel(p);
            if (ch == null) return List.of();
            rb.header("Authorization", "Bearer " + ch.getApiKey());
        }
        HttpResponse<String> resp = providerClients.forProvider(p, CONNECT_TIMEOUT_MS).send(rb.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            String bodySnippet = resp.body() == null ? "" : resp.body().replaceAll("[\r\n]", " ");
            if (bodySnippet.length() > 160) bodySnippet = bodySnippet.substring(0, 160) + "…";
            throw new FetchException(new FetchResult(resp.statusCode(), bodySnippet),
                    "HTTP " + resp.statusCode() + (bodySnippet.isEmpty() ? "" : ": " + bodySnippet));
        }
        return parseModelsJson(resp.body());
    }

    private GatewayConfig.Channel firstEnabledChannel(GatewayConfig.Provider p) {
        return p.getChannels().stream().filter(GatewayConfig.Channel::isEnabled).findFirst().orElse(null);
    }

    /** 极简 JSON 解析：抽取 "id":"xxx" 字段（模型清单格式各家统一是 data[].id） */
    static List<String> parseModelsJson(String body) {
        List<String> out = new ArrayList<>();
        int i = 0;
        while (true) {
            int idIdx = body.indexOf("\"id\"", i);
            if (idIdx < 0) break;
            int colon = body.indexOf(':', idIdx);
            if (colon < 0) break;
            int q1 = body.indexOf('"', colon);
            if (q1 < 0) break;
            int q2 = body.indexOf('"', q1 + 1);
            if (q2 < 0) break;
            String v = body.substring(q1 + 1, q2);
            if (!v.isBlank()) out.add(v);
            i = q2 + 1;
        }
        return out;
    }

    /** 查询：支持该模型的提供商集合 */
    public Set<String> providersOf(String model) {
        return catalog.getOrDefault(model.toLowerCase(), Set.of());
    }

    public Map<String, Set<String>> fullCatalog() { return catalog; }

    public boolean isStale(String providerId) { return stale.getOrDefault(providerId, false); }
    public Long lastFetch(String providerId) { return lastFetchMs.get(providerId); }

    /** 供管理页"查上游模型"用：实时拉指定供应商的 /v1/models（不复用缓存，不污染目录） */
    public List<String> fetchUpstreamModelsNow(GatewayConfig.Provider p) throws Exception {
        GatewayConfig cfg = configService.snapshot().config;
        return fetchProviderModels(p, cfg.getDefaults().getCatalogFetchTimeoutMs());
    }

    /** 同上，但返回原始响应体（含各家扩展字段如 pricing）——查上游弹窗显示价格用 */
    public String fetchUpstreamModelsRaw(GatewayConfig.Provider p) throws Exception {
        GatewayConfig cfg = configService.snapshot().config;
        String url = p.getBaseUrl().replaceAll("/+$", "") + "/models";
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(cfg.getDefaults().getCatalogFetchTimeoutMs()))
                .GET();
        GatewayConfig.Channel ch = firstEnabledChannel(p);
        if (ch == null) return "{}";
        if ("anthropic".equals(p.getProtocol())) {
            rb.header("x-api-key", ch.getApiKey()).header("anthropic-version", "2023-06-01");
        } else {
            rb.header("Authorization", "Bearer " + ch.getApiKey());
        }
        HttpResponse<String> resp = providerClients.forProvider(p, CONNECT_TIMEOUT_MS).send(rb.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            String bodySnippet = resp.body() == null ? "" : resp.body().replaceAll("[\r\n]", " ");
            if (bodySnippet.length() > 160) bodySnippet = bodySnippet.substring(0, 160) + "…";
            throw new FetchException(new FetchResult(resp.statusCode(), bodySnippet),
                    "HTTP " + resp.statusCode() + (bodySnippet.isEmpty() ? "" : ": " + bodySnippet));
        }
        return resp.body() == null ? "{}" : resp.body();
    }

    // ================= 账户余额（管理页"查上游"顺带查询，数据驱动、各家异构端点走配置） =================

    /** 余额查询结果：ok=false 时 error 说明原因（未配置/无 Key/HTTP 失败/字段缺失）。 */
    public record BalanceResult(boolean ok, String amount, String currency, String error) {}

    /** 用首条启用渠道 Key GET 余额端点，按 fields 抽数字（可减 subtractFields）→ 展示金额。
     *  任何失败都不抛出，转成 ok=false + error，避免拖垮"查上游"整体响应。 */
    public BalanceResult fetchBalance(GatewayConfig.Provider p) {
        GatewayConfig.BalanceProbe bp = p.getBalance();
        if (bp == null || bp.getPath() == null || bp.getPath().isBlank())
            return new BalanceResult(false, null, null, "未配置余额端点");
        GatewayConfig.Channel ch = firstEnabledChannel(p);
        if (ch == null) return new BalanceResult(false, null, null, "无启用的渠道 Key");
        try {
            String base = p.getBaseUrl() == null ? "" : p.getBaseUrl().replaceAll("/+$", "");
            String path = bp.getPath();
            String url = path.startsWith("http") ? path : base + (path.startsWith("/") ? path : "/" + path);
            int timeoutMs = configService.snapshot().config.getDefaults().getCatalogFetchTimeoutMs();
            HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs)).GET();
            if ("anthropic".equals(p.getProtocol())) {
                rb.header("x-api-key", ch.getApiKey()).header("anthropic-version", "2023-06-01");
            } else {
                rb.header("Authorization", "Bearer " + ch.getApiKey());
            }
            HttpResponse<String> resp = providerClients.forProvider(p, CONNECT_TIMEOUT_MS).send(rb.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                String snip = resp.body() == null ? "" : resp.body().replaceAll("[\r\n]", " ");
                if (snip.length() > 120) snip = snip.substring(0, 120) + "…";
                return new BalanceResult(false, null, null,
                        "HTTP " + resp.statusCode() + (snip.isEmpty() ? "" : ": " + snip));
            }
            Object root = Json.parse(resp.body() == null ? "{}" : resp.body());
            Double amount = null;
            for (String f : nz(bp.getFields())) {
                Double d = toNum(extractJsonPath(root, f));
                if (d != null) { amount = d; break; }
            }
            if (amount == null) return new BalanceResult(false, null, null, "响应中未找到余额字段");
            for (String f : nz(bp.getSubtractFields())) {
                Double d = toNum(extractJsonPath(root, f));
                if (d != null) amount -= d;
            }
            String currency = null;
            if (bp.getCurrencyField() != null && !bp.getCurrencyField().isBlank()) {
                Object c = extractJsonPath(root, bp.getCurrencyField());
                if (c != null) currency = String.valueOf(c);
            }
            if (currency == null) currency = bp.getUnit();
            return new BalanceResult(true, fmtAmount(amount), currency, null);
        } catch (Exception e) {
            return new BalanceResult(false, null, null, "查询失败: " + e.getMessage());
        }
    }

    /** 点分路径取值：支持对象键与数组下标（如 balance_infos.0.total_balance）。取不到返回 null。纯函数，便于单测。 */
    static Object extractJsonPath(Object node, String path) {
        if (node == null || path == null || path.isBlank()) return null;
        Object cur = node;
        for (String seg : path.split("\\.")) {
            if (seg.isEmpty()) continue;
            if (cur instanceof Map) {
                cur = ((Map<?, ?>) cur).get(seg);
            } else if (cur instanceof List) {
                int idx;
                try { idx = Integer.parseInt(seg); } catch (NumberFormatException e) { return null; }
                List<?> list = (List<?>) cur;
                cur = (idx >= 0 && idx < list.size()) ? list.get(idx) : null;
            } else {
                return null;
            }
            if (cur == null) return null;
        }
        return cur;
    }

    /** 数字宽松解析：Number 直取；String 去空格后按 double 解析（deepseek 的 "103.44"）。非数字返回 null。 */
    static Double toNum(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).doubleValue();
        try { return Double.parseDouble(String.valueOf(v).trim()); } catch (NumberFormatException e) { return null; }
    }

    /** 金额格式化：整数值不带小数，否则保留两位。 */
    static String fmtAmount(double amt) {
        if (amt == Math.rint(amt) && !Double.isInfinite(amt) && Math.abs(amt) < 1e15)
            return String.valueOf((long) amt);
        return String.format("%.2f", amt);
    }

    private static List<String> nz(List<String> l) { return l == null ? List.of() : l; }
}
