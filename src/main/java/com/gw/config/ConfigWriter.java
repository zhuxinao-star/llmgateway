package com.gw.config;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;

/**
 * 配置写回：GatewayConfig → YAML（kebab-case，可读格式 + 手写注释头）。
 * 不用 SnakeYAML dump（类标签/命名风格导致重载失败），手动序列化保证 round-trip。
 * 原子写：先写临时文件再替换，避免写一半崩溃损坏配置。
 */
public final class ConfigWriter {

    private ConfigWriter() {}

    public static void write(GatewayConfig c, Path target) throws Exception {
        StringBuilder y = new StringBuilder(4096);
        y.append("# ============================================================\n");
        y.append("# LLM Gateway 配置（管理页生成）\n");
        y.append("# 优先级：数字小者优先；费用档 free/low/standard/premium\n");
        y.append("# ============================================================\n\n");

        y.append("server:\n");
        y.append("  bind: ").append(c.getServer().getBind()).append('\n');
        y.append("  port: ").append(c.getServer().getPort()).append('\n');
        if (c.getServer().getDbPath() != null && !c.getServer().getDbPath().isBlank())
            y.append("  dbPath: ").append(q(c.getServer().getDbPath())).append('\n');
        y.append("\ngateway-keys:\n");
        for (String k : c.getGatewayKeys()) y.append("  - ").append(q(k)).append('\n');
        if (c.getAdminToken() != null && !c.getAdminToken().isBlank())
            y.append("\nadmin-token: ").append(q(c.getAdminToken().trim())).append('\n');
        y.append("\nstrategy: ").append(c.getStrategy()).append('\n');

        // 来源工具展示白名单（空=不写，下拉展示全部来源）
        List<String> lc = c.getLogClients();
        if (lc != null && !lc.isEmpty()) {
            y.append("\nlog-clients:\n");
            for (String s : lc) y.append("  - ").append(q(s)).append('\n');
        }

        GatewayConfig.Defaults d = c.getDefaults();
        y.append("\ndefaults:\n");
        y.append("  connectTimeoutMs: ").append(d.getConnectTimeoutMs()).append('\n');
        y.append("  responseHeaderTimeoutMs: ").append(d.getResponseHeaderTimeoutMs()).append('\n');
        y.append("  ttftTimeoutMs: ").append(d.getTtftTimeoutMs()).append('\n');
        y.append("  channelBudgetMs: ").append(d.getChannelBudgetMs()).append('\n');
        y.append("  streamHeartbeatTimeoutMs: ").append(d.getStreamHeartbeatTimeoutMs()).append('\n');
        y.append("  firstTokenHeartbeatTimeoutMs: ").append(d.getFirstTokenHeartbeatTimeoutMs()).append('\n');
        y.append("  totalBudgetMs: ").append(d.getTotalBudgetMs()).append('\n');
        y.append("  compressCallTimeoutMs: ").append(d.getCompressCallTimeoutMs()).append('\n');
        y.append("  compressOverallBudgetMs: ").append(d.getCompressOverallBudgetMs()).append('\n');
        y.append("  maxAttempts: ").append(d.getMaxAttempts()).append('\n');
        y.append("  circuitBreakerThreshold: ").append(d.getCircuitBreakerThreshold()).append('\n');
        y.append("  circuitBreakerBaseCooldownMs: ").append(d.getCircuitBreakerBaseCooldownMs()).append('\n');
        y.append("  circuitBreakerMaxCooldownMs: ").append(d.getCircuitBreakerMaxCooldownMs()).append('\n');
        y.append("  rateLimitCooldownMs: ").append(d.getRateLimitCooldownMs()).append('\n');
        y.append("  quotaCooldownMs: ").append(d.getQuotaCooldownMs()).append('\n');
        y.append("  catalogRefreshMinutes: ").append(d.getCatalogRefreshMinutes()).append('\n');
        y.append("  catalogFetchTimeoutMs: ").append(d.getCatalogFetchTimeoutMs()).append('\n');
        y.append("  ewmaAlpha: ").append(d.getEwmaAlpha()).append('\n');
        y.append("  hedgeDelayMs: ").append(d.getHedgeDelayMs()).append('\n');
        y.append("  hedgeMaxContextTokens: ").append(d.getHedgeMaxContextTokens()).append('\n');
        y.append("  contextCompressThresholdTokens: ").append(d.getContextCompressThresholdTokens()).append('\n');
        if (d.getContextCompressModel() != null && !d.getContextCompressModel().isBlank())
            y.append("  contextCompressModel: ").append(d.getContextCompressModel()).append('\n');
        y.append("  keepRecentMessages: ").append(d.getKeepRecentMessages()).append('\n');
        y.append("  keepRecentTokens: ").append(d.getKeepRecentTokens()).append('\n');
        y.append("  probeIntervalMinutes: ").append(d.getProbeIntervalMinutes()).append('\n');
        y.append("  probeMaxTokens: ").append(d.getProbeMaxTokens()).append('\n');
        y.append("  clientMaxConcurrency: ").append(d.getClientMaxConcurrency()).append('\n');
        y.append("  clientMaxConcurrencyWaitMs: ").append(d.getClientMaxConcurrencyWaitMs()).append('\n');
        y.append("  loadShare: ").append(d.isLoadShare()).append('\n');
        y.append("  shareBandSize: ").append(d.getShareBandSize()).append('\n');
        y.append("  shareSlowTtftMs: ").append(d.getShareSlowTtftMs()).append('\n');
        // catalogDiscovery 默认 false，仅在开启时写（避免污染默认配置；false 是安全默认）
        if (d.isCatalogDiscovery()) y.append("  catalogDiscovery: true\n");
        // 模型自动停用/恢复：默认关，仅在开启或调参时写（false 是安全默认，不污染配置）
        if (d.isAutoDisableEnabled()) y.append("  autoDisableEnabled: true\n");
        if (d.getReconcileIntervalMinutes() != 60) y.append("  reconcileIntervalMinutes: ").append(d.getReconcileIntervalMinutes()).append('\n');
        if (d.getReconcileMinFailures() != 2) y.append("  reconcileMinFailures: ").append(d.getReconcileMinFailures()).append('\n');
        if (d.getReconcileProbeBudget() != 5) y.append("  reconcileProbeBudget: ").append(d.getReconcileProbeBudget()).append('\n');
        // 下游 SSE 保活间隔：默认 5000 不写（默认即开启保活）；仅调参/关闭时写
        if (d.getSseKeepAliveIntervalMs() != 5000) y.append("  sseKeepAliveIntervalMs: ").append(d.getSseKeepAliveIntervalMs()).append('\n');
        // 路由决策留痕：默认（开 + 5% + 7 天）不写；偏离默认才写，保证界面/手工改过的值能存住
        if (!d.isRankTraceEnabled()) y.append("  rankTraceEnabled: false\n");
        if (d.getRankTraceSampleRate() != 0.05) y.append("  rankTraceSampleRate: ").append(d.getRankTraceSampleRate()).append('\n');
        if (d.getRankTraceKeepDays() != 7) y.append("  rankTraceKeepDays: ").append(d.getRankTraceKeepDays()).append('\n');

        // providers
        y.append("\nproviders:\n");
        for (GatewayConfig.Provider p : c.getProviders()) {
            y.append("  - id: ").append(p.getId()).append('\n');
            y.append("    name: ").append(q(p.getName())).append('\n');
            y.append("    baseUrl: ").append(p.getBaseUrl()).append('\n');
            y.append("    protocol: ").append(p.getProtocol()).append('\n');
            y.append("    priority: ").append(p.getPriority()).append('\n');
            y.append("    defaultCostTier: ").append(p.getDefaultCostTier()).append('\n');
            if (p.getProxy() != null && !p.getProxy().isBlank())
                y.append("    proxy: ").append(p.getProxy()).append('\n');
            if (p.getThinkingMode() != null && !"auto".equals(p.getThinkingMode()))
                y.append("    thinkingMode: ").append(p.getThinkingMode()).append('\n');
            if (!p.isEnabled()) y.append("    enabled: false\n");
            if (p.getDailyTokenLimit() != null)
                y.append("    dailyTokenLimit: ").append(p.getDailyTokenLimit()).append('\n');
            if (p.getConcurrencyLimit() != null)
                y.append("    concurrencyLimit: ").append(p.getConcurrencyLimit()).append('\n');
            y.append("    channels:\n");
            for (GatewayConfig.Channel ch : p.getChannels()) {
                y.append("      - id: ").append(ch.getId()).append('\n');
                y.append("        apiKey: ").append(q(ch.getApiKey())).append('\n');
                if (ch.getWeight() != 1) y.append("        weight: ").append(ch.getWeight()).append('\n');
                if (!ch.isEnabled()) y.append("        enabled: false\n");
                if (ch.getDailyTokenLimit() != null)
                    y.append("        dailyTokenLimit: ").append(ch.getDailyTokenLimit()).append('\n');
                if (ch.getConcurrencyLimit() != null)
                    y.append("        concurrencyLimit: ").append(ch.getConcurrencyLimit()).append('\n');
            }
            List<String> sm = p.getStaticModels();
            if (sm != null && !sm.isEmpty()) {
                y.append("    static-models: [").append(joinQuoted(sm)).append("]\n");
            }
            GatewayConfig.BalanceProbe bp = p.getBalance();
            if (bp != null && bp.getPath() != null && !bp.getPath().isBlank()) {
                y.append("    balance:\n");
                y.append("      path: ").append(bp.getPath()).append('\n');
                if (bp.getFields() != null && !bp.getFields().isEmpty())
                    y.append("      fields: [").append(String.join(", ", bp.getFields())).append("]\n");
                if (bp.getSubtractFields() != null && !bp.getSubtractFields().isEmpty())
                    y.append("      subtract-fields: [").append(String.join(", ", bp.getSubtractFields())).append("]\n");
                if (bp.getCurrencyField() != null && !bp.getCurrencyField().isBlank())
                    y.append("      currency-field: ").append(bp.getCurrencyField()).append('\n');
                if (bp.getUnit() != null && !bp.getUnit().isBlank())
                    y.append("      unit: ").append(q(bp.getUnit())).append('\n');
            }
        }

        // routing-overrides（空=不写节，避免重载时 null）
        if (!c.getRoutingOverrides().isEmpty()) {
        y.append("\nrouting-overrides:\n");
        for (GatewayConfig.RoutingOverride ro : c.getRoutingOverrides()) {
            y.append("  - { providerId: ").append(ro.getProviderId())
                    .append(", model: ").append(q(ro.getModel()));
            if (ro.getPriority() != null) y.append(", priority: ").append(ro.getPriority());
            if (ro.getCostTier() != null) y.append(", costTier: ").append(ro.getCostTier());
            if (ro.getDailyTokenLimit() != null) y.append(", dailyTokenLimit: ").append(ro.getDailyTokenLimit());
            if (ro.getConcurrencyLimit() != null) y.append(", concurrencyLimit: ").append(ro.getConcurrencyLimit());
            if (ro.getAlias() != null && !ro.getAlias().isBlank()) y.append(", alias: ").append(q(ro.getAlias()));
            if (ro.getCategory() != null && !ro.getCategory().isBlank()) y.append(", category: ").append(ro.getCategory());
            if (ro.getContextWindow() != null) y.append(", contextWindow: ").append(ro.getContextWindow());
            if (Boolean.TRUE.equals(ro.getExcludeFromAutoAll())) y.append(", excludeFromAutoAll: true");
            if (ro.getCodeScore() != null) y.append(", codeScore: ").append(ro.getCodeScore());
            if (ro.getUserWeight() != null) y.append(", userWeight: ").append(ro.getUserWeight());
            if (Boolean.TRUE.equals(ro.getToolWeak())) y.append(", toolWeak: true");
            if (Boolean.TRUE.equals(ro.getExcludeFromProbe())) y.append(", excludeFromProbe: true");
            if (Boolean.FALSE.equals(ro.getEnabled())) y.append(", enabled: false");
            if (Boolean.TRUE.equals(ro.getAutoDisabled())) y.append(", autoDisabled: true");
            y.append(" }\n");
        }
        }

        // groups
        if (!c.getGroups().isEmpty()) {
            y.append("\ngroups:\n");
            for (GatewayConfig.Group g : c.getGroups()) {
                y.append("  - name: ").append(g.getName()).append('\n');
                if (g.isAutoAll()) y.append("    autoAll: true\n");
                if (!g.getMembers().isEmpty()) {
                    y.append("    members:\n");
                    for (GatewayConfig.Group.Member m : g.getMembers()) {
                        y.append("      - { providerId: ").append(m.getProviderId())
                                .append(", model: ").append(q(m.getModel()));
                        if (m.getPriority() != null) y.append(", priority: ").append(m.getPriority());
                        y.append(" }\n");
                    }
                } else if (!g.isAutoAll()) {
                    y.append("    members: []\n");
                }
            }
        }

        // 原子替换：Windows 下 Files.move+REPLACE 常因文件被占（杀软/同步打开）失败，
        // 拆成"先改名 .bak 再搬入"并带重试——直接 deleteIfExists(target) 再 move 的旧写法
        // 存在删除成功、搬入前进程死亡的窗口，config 只剩 .tmp，全部上游 Key 即丢
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(tmp, y.toString(), StandardCharsets.UTF_8);
        Exception last = null;
        for (int i = 0; i < 3; i++) {
            try {
                try {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException | java.nio.file.AccessDeniedException e) {
                    Path bak = target.resolveSibling(target.getFileName() + ".bak");
                    Files.deleteIfExists(bak);
                    if (Files.exists(target)) Files.move(target, bak, StandardCopyOption.REPLACE_EXISTING);
                    try {
                        Files.move(tmp, target);
                        Files.deleteIfExists(bak);
                    } catch (Exception moveErr) {
                        // 新文件没搬进去 → 先把 .bak 还原，旧配置（含全部 Key）照常可用
                        if (Files.exists(bak) && !Files.exists(target)) Files.move(bak, target);
                        throw moveErr;
                    }
                }
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(200);
            }
        }
        throw last;
    }

    private static String joinQuoted(List<String> items) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append('"').append(items.get(i)).append('"');
        }
        return sb.toString();
    }

    /** 需要引号的值（含 / : 等特殊字符或空值）；引号/反斜杠/换行必须转义，
     *  否则 apiKey 含 " 或 \ 时写出的 yaml 直接坏掉（load 失败=全部 Key 失效） */
    private static String q(String v) {
        if (v == null || v.isEmpty()) return "\"\"";
        boolean needQuote = v.contains("/") || v.contains(":") || v.contains(" ") || v.contains("#")
                || v.indexOf('"') >= 0 || v.indexOf('\\') >= 0
                || v.indexOf('\n') >= 0 || v.indexOf('\r') >= 0 || v.indexOf('\t') >= 0
                || v.startsWith("'");
        if (!needQuote) return v;
        String esc = v.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
        return "\"" + esc + "\"";
    }
}
