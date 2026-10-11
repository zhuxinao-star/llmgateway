package com.gw.router;

import com.gw.catalog.CatalogService;
import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.policy.PolicyService;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 路由模块：model 名 → 候选 RoutingEntry 列表。
 * - 直接名称匹配：所有支持该模型的提供商都进候选池
 * - 前缀点名：`provider/model` 强制走某家
 * - 虚拟模型：组名展开为成员列表
 */
@Service
public class RouterService {

    private final ConfigService configService;
    private final CatalogService catalogService;
    private final PolicyService policyService;

    public RouterService(ConfigService configService, CatalogService catalogService, PolicyService policyService) {
        this.configService = configService;
        this.catalogService = catalogService;
        this.policyService = policyService;
    }

    /**
     * 解析请求 model 字段并给出候选列表（已含该提供商所有 enabled 渠道展开）。
     * @return 候选；空 = model 不存在
     */
    public List<PolicyService.Candidate> resolve(String requestedModel) {
        ConfigService.Snapshot snap = configService.snapshot();
        if (requestedModel == null || requestedModel.isBlank()) return List.of();
        // 大小写归一：Claude Code 等客户端偶尔大小写不一致，精确匹配失败会落进"模型不存在→默认组"
        // 静默换模型；且上游真名区分大小写（AMD 教训：Kimi-K2 发小写直接 model_not_found）
        String canon = canonicalize(snap, requestedModel);
        if (canon != null) requestedModel = canon;

        // 1) 前缀点名 provider/model
        int slash = requestedModel.indexOf('/');
        if (slash > 0) {
            String prefix = requestedModel.substring(0, slash);
            String realModel = requestedModel.substring(slash + 1);
            String pid = snap.providersById.containsKey(prefix) ? prefix
                    : ciFind(snap.providersById.keySet(), prefix);
            if (pid != null) {
                // 模型部分同样大小写归一（static 清单与路由条目名两处候选）
                GatewayConfig.Provider pv = snap.providersById.get(pid);
                for (String m : pv.getStaticModels())
                    if (m.equalsIgnoreCase(realModel)) { realModel = m; break; }
                for (GatewayConfig.RoutingOverride ro : snap.config.getRoutingOverrides())
                    if (ro.getProviderId().equals(pid) && ro.getModel() != null && realModel.equalsIgnoreCase(ro.getModel()))
                        { realModel = ro.getModel(); break; }
                return candidatesFor(snap, pid, realModel, null);
            }
            // 前缀不是提供商 id → 落到组名/普通匹配逻辑（前缀含 / 但不是 provider 的组名形式不支持）
        }

        // 2) 虚拟模型（组名）
        for (GatewayConfig.Group g : snap.config.getGroups()) {
            if (g.getName().equals(requestedModel)) {
                List<PolicyService.Candidate> out = new ArrayList<>();
                if (g.isAutoAll()) {
                    // 自动组：所有启用供应商的全部手工配置模型（static-models），
                    // 组内优先级沿用路由覆盖值（无覆盖则用供应商基础值），免费/快者自然排前。
                    // excludeFromAutoAll 的条目跳过（图像端点专用模型聊天必失败，不进聊天候选池）
                    for (GatewayConfig.Provider p : snap.config.getProviders()) {
                        if (!p.isEnabled()) continue;
                        for (String m : p.getStaticModels()) {
                            if (snap.excludedFromAutoAll(p.getId(), m)) continue;
                            out.addAll(candidatesFor(snap, p.getId(), m, null));
                        }
                    }
                }
                for (GatewayConfig.Group.Member m : g.getMembers()) {
                    GatewayConfig.Provider p = snap.providersById.get(m.getProviderId());
                    if (p == null || !p.isEnabled()) continue;
                    out.addAll(candidatesFor(snap, m.getProviderId(), m.getModel(), m.getPriority()));
                }
                return out;
            }
        }

        // 3) 直接模型名匹配（目录自动发现 + 静态清单）
        Set<String> providerIds = new LinkedHashSet<>(catalogService.providersOf(requestedModel));
        // 静态清单的提供商也通过 catalog 已包含；但目录尚未拉完时的兜底：扫配置静态清单
        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            if (p.getStaticModels().contains(requestedModel)) providerIds.add(p.getId());
        }
        List<PolicyService.Candidate> out = new ArrayList<>();
        for (String pid : providerIds) {
            out.addAll(candidatesFor(snap, pid, requestedModel, null));
        }
        return out;
    }

    /** 大小写归一：请求名与已知规范名（组名/static 清单/路由条目/目录）精确命中→原样；
     *  只有 equalsIgnoreCase 命中→返回配置里的规范拼写（转发上游用真名）。无候选返回 null（保持原名走 404 逻辑）。 */
    private String canonicalize(ConfigService.Snapshot snap, String requested) {
        for (GatewayConfig.Group g : snap.config.getGroups())
            if (g.getName().equals(requested)) return requested;
        for (GatewayConfig.Provider p : snap.config.getProviders())
            if (p.getStaticModels().contains(requested)) return requested;
        for (GatewayConfig.RoutingOverride ro : snap.config.getRoutingOverrides())
            if (requested.equals(ro.getModel())) return requested;
        // —— 以下忽略大小写找规范名（static 优先：那是手工核对过的上游真名拼写）——
        for (GatewayConfig.Provider p : snap.config.getProviders())
            for (String m : p.getStaticModels())
                if (m.equalsIgnoreCase(requested)) return m;
        for (GatewayConfig.RoutingOverride ro : snap.config.getRoutingOverrides())
            if (ro.getModel() != null && requested.equalsIgnoreCase(ro.getModel())) return ro.getModel();
        for (GatewayConfig.Group g : snap.config.getGroups())
            if (g.getName().equalsIgnoreCase(requested)) return g.getName();
        // 目录名（自动发现来或别名兜底）本就按小写索引，大小写不敏感命中 → 原样放行给后续匹配
        if (!catalogService.providersOf(requested).isEmpty()) return requested;
        return null;
    }

    private static String ciFind(Collection<String> names, String needle) {
        for (String n : names) if (n.equalsIgnoreCase(needle)) return n;
        return null;
    }

    /** 某提供商某模型 → 其所有 enabled 渠道各一条候选（多 Key 轮转分摊）。
     *  模型被停用（routing-overrides enabled:false）→ 空候选：点名请求 404 走默认组回落，
     *  组/autoAll 展开则天然跳过该模型（供应商仍启用，其它模型照常路由）。 */
    private List<PolicyService.Candidate> candidatesFor(ConfigService.Snapshot snap,
                                                        String providerId, String model, Integer groupPriority) {
        List<PolicyService.Candidate> out = new ArrayList<>();
        if (snap.modelDisabled(providerId, model)) return out;
        for (GatewayConfig.Channel ch : snap.enabledChannels(providerId)) {
            out.add(new PolicyService.Candidate(providerId, model, ch.getId(),
                    Math.max(1, ch.getWeight()), groupPriority));
        }
        return out;
    }

    /** /v1/models 聚合响应：原始名 + provider/model 全名 + 组名（停用的模型不列出） */
    public List<Map<String, Object>> aggregatedModels() {
        ConfigService.Snapshot snap = configService.snapshot();
        Map<String, Set<String>> cat = catalogService.fullCatalog();
        List<Map<String, Object>> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        long created = System.currentTimeMillis() / 1000;
        for (Map.Entry<String, Set<String>> e : cat.entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).toList()) {
            boolean anyEnabled = false;
            for (String pid : e.getValue()) {
                if (snap.modelDisabled(pid, e.getKey())) continue;   // 停用的模型不对客户端展示同名全名条目
                anyEnabled = true;
                String full = pid + "/" + e.getKey();
                if (seen.add(full)) {
                    out.add(Map.of("id", full, "object", "model", "created", created, "owned_by", pid));
                }
            }
            if (anyEnabled && seen.add(e.getKey())) {
                out.add(Map.of("id", e.getKey(), "object", "model", "created", created, "owned_by", "gateway"));
            }
        }
        for (GatewayConfig.Group g : configService.snapshot().config.getGroups()) {
            out.add(Map.of("id", g.getName(), "object", "model", "created", created, "owned_by", "gateway-group"));
        }
        return out;
    }
}
