package com.gw.admin;

import com.gw.catalog.CatalogService;
import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.forward.ForwardEngine;
import com.gw.policy.PolicyService;
import com.gw.provider.adaptor.Json;
import com.gw.stats.StatsService;
import com.gw.store.StatsDao;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 管理 REST（/api/*）：
 * - 供应商/渠道/路由条目/分组的 增删改（持久化写回 config.yaml）
 * - 用量聚合（每供应商每模型 请求次数/token 消耗；按接入工具维度）
 * - 请求日志分页、策略切换、预算重置、目录刷新、配置热重载
 * - 模型连通性测试（直连单供应商×模型，回显模型输出）
 */
@RestController
@RequestMapping("/api")
public class AdminController {

    private final ConfigService configService;
    private final CatalogService catalogService;
    private final PolicyService policyService;
    private final StatsService statsService;
    private final StatsDao statsDao;
    private final ForwardEngine forwardEngine;
    private final com.gw.config.ApiTimingFilter apiTimingFilter;
    private final com.gw.router.RouterService routerService;
    private final com.gw.policy.RankTraceStore rankTraceStore;

    public AdminController(ConfigService configService, CatalogService catalogService,
                           PolicyService policyService, StatsService statsService,
                           StatsDao statsDao, ForwardEngine forwardEngine,
                           com.gw.config.ApiTimingFilter apiTimingFilter,
                           com.gw.router.RouterService routerService,
                           com.gw.policy.RankTraceStore rankTraceStore) {
        this.configService = configService;
        this.catalogService = catalogService;
        this.policyService = policyService;
        this.statsService = statsService;
        this.statsDao = statsDao;
        this.forwardEngine = forwardEngine;
        this.apiTimingFilter = apiTimingFilter;
        this.routerService = routerService;
        this.rankTraceStore = rankTraceStore;
    }

    // ================= 总览 =================

    @GetMapping("/overview")
    public Map<String, Object> overview() {
        Map<String, Object> out = new LinkedHashMap<>();
        ConfigService.Snapshot snap = configService.snapshot();
        out.put("strategy", snap.config.getStrategy());
        out.put("configLoadedAt", snap.loadedAtMs);
        out.put("catalogSize", catalogService.fullCatalog().size());
        out.put("today", policyService.today());

        List<Map<String, Object>> providers = new ArrayList<>();
        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            Map<String, Object> pv = new LinkedHashMap<>();
            pv.put("id", p.getId());
            pv.put("name", p.getName());
            pv.put("protocol", p.getProtocol());
            pv.put("baseUrl", p.getBaseUrl());
            pv.put("proxy", p.getProxy());
            pv.put("thinkingMode", p.getThinkingMode());
            pv.put("priority", p.getPriority());
            pv.put("enabled", p.isEnabled());
            pv.put("costTier", p.getDefaultCostTier());
            pv.put("dailyTokenLimit", p.getDailyTokenLimit());
            pv.put("concurrencyLimit", p.getConcurrencyLimit());
            String pid = p.getId();
            List<Map<String, Object>> chs = new ArrayList<>();
            for (GatewayConfig.Channel ch : p.getChannels()) {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("id", ch.getId());
                v.put("enabled", ch.isEnabled());
                v.put("weight", ch.getWeight());
                v.put("keyMasked", mask(ch.getApiKey()));
                v.put("dailyTokenLimit", ch.getDailyTokenLimit());
                // 今日该 Key 实流量（内存日计数器，重启清零）：一家多把 Key 时不必翻页就能看出偏斜
                v.put("todayRequests", policyService.requestsTodayChannel(pid, ch.getId()));
                v.put("todayTokens", policyService.tokensUsedChannel(pid, ch.getId()));
                chs.add(v);
            }
            pv.put("channels", chs);
            pv.put("staticModels", p.getStaticModels());
            // 模型清单 = 静态清单 ∪ 该供应商的路由覆盖条目（发现开关关闭时目录只有静态清单，
            // 开启时目录额外并入，带 "自动" 标记便于区分来源）
            java.util.Set<String> staticSet = new java.util.HashSet<>(p.getStaticModels() == null
                    ? List.of() : p.getStaticModels());
            java.util.Set<String> modelNames = new java.util.LinkedHashSet<>(staticSet);
            if (snap.config.getDefaults().isCatalogDiscovery()) {
                for (Map.Entry<String, java.util.Set<String>> en : catalogService.fullCatalog().entrySet()) {
                    if (en.getValue().contains(pid)) modelNames.add(en.getKey());
                }
            }
            // 路由覆盖里引用的模型（可能既不在静态清单也未被发现）
            java.util.Map<String, GatewayConfig.RoutingOverride> roByModel = new java.util.LinkedHashMap<>();
            for (GatewayConfig.RoutingOverride ro : snap.config.getRoutingOverrides()) {
                if (ro.getProviderId().equals(pid)) {
                    roByModel.put(ro.getModel(), ro);
                    modelNames.add(ro.getModel());
                }
            }
            // 归并重复：override 指向上游真名(alias)，static-models 里存的往往就是那个真名
            // （如请求名 qwen3.8-flash-next + alias Qwen3.8-Flash-Next + static 含 Qwen3.8-Flash-Next）。
            // 展示为一行：请求名（配置名）+ 上游真名（alias），避免同模型两行混淆。
            // 例外：alias 指向自己（请求名=真名，如 MiniCPM5-2B）不参与吞并——
            // 否则 removeIf 会把这行也当成"被合并的重复行"删掉，导致卡片上消失。
            java.util.Set<String> aliasNames = new java.util.HashSet<>();
            for (GatewayConfig.RoutingOverride ro : roByModel.values()) {
                String al = ro.getAlias();
                if (al != null && !al.isBlank() && !al.equals(ro.getModel())) aliasNames.add(al);
            }
            modelNames.removeIf(aliasNames::contains);
            List<Map<String, Object>> models = new ArrayList<>();
            for (String m : modelNames) {
                GatewayConfig.RoutingOverride ro = roByModel.get(m);
                // 合并行补看：探测豁免等按 static 真名生效（探测循环用的是真名，如 openrouter/free），
                // 展示须并入 alias 条目的状态——否则界面永远显示未勾选，用户保存该行还会把豁免静默清掉
                GatewayConfig.RoutingOverride aliasRo = ro == null || ro.getAlias() == null
                        || ro.getAlias().isBlank() || ro.getAlias().equals(m)
                        ? null : roByModel.get(ro.getAlias());
                Map<String, Object> mv = new LinkedHashMap<>();
                mv.put("model", m);
                mv.put("source", staticSet.contains(m) ? "static" : (ro != null ? "override" : "discovered"));
                mv.put("priority", ro != null && ro.getPriority() != null ? ro.getPriority() : p.getPriority());
                mv.put("costTier", ro != null && ro.getCostTier() != null ? ro.getCostTier() : p.getDefaultCostTier());
                mv.put("dailyTokenLimit", ro == null ? null : ro.getDailyTokenLimit());
                mv.put("alias", ro == null ? null : ro.getAlias());
                mv.put("category", ro == null ? null : ro.getCategory());
                mv.put("contextWindow", ro == null ? null : ro.getContextWindow());
                mv.put("excludeFromAutoAll", ro != null && Boolean.TRUE.equals(ro.getExcludeFromAutoAll()));
                mv.put("codeScore", ro == null ? null : ro.getCodeScore());
                mv.put("userWeight", ro == null ? null : ro.getUserWeight());
                mv.put("toolWeak", ro != null && Boolean.TRUE.equals(ro.getToolWeak()));
                mv.put("excludeFromProbe", (ro != null && Boolean.TRUE.equals(ro.getExcludeFromProbe()))
                        || (aliasRo != null && Boolean.TRUE.equals(aliasRo.getExcludeFromProbe())));
                mv.put("enabled", !snap.modelDisabled(pid, m));
                mv.put("requestsToday", policyService.requestsToday(pid, m));
                mv.put("tokensToday", policyService.tokensUsed(pid, m));
                models.add(mv);
            }
            // 优先级小者在前，便于卡片按路由顺序展示
            models.sort(Comparator.comparingInt(mv -> (int) mv.get("priority")));
            pv.put("models", models);
            pv.put("catalogStale", catalogService.isStale(p.getId()));
            pv.put("catalogFetchedAt", catalogService.lastFetch(p.getId()));
            providers.add(pv);
        }
        out.put("providers", providers);
        out.put("groups", groupViews());
        out.put("runtime", policyService.runtimeView());
        out.put("usage", statsService.todayUsageView());
        return out;
    }

    static String mask(String key) {
        if (key == null || key.length() < 8) return "****";
        return key.substring(0, 4) + "…" + key.substring(key.length() - 4);
    }

    private List<Map<String, Object>> groupViews() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (GatewayConfig.Group g : configService.snapshot().config.getGroups()) {
            Map<String, Object> gv = new LinkedHashMap<>();
            gv.put("name", g.getName());
            gv.put("autoAll", g.isAutoAll());
            List<Map<String, Object>> ms = new ArrayList<>();
            for (GatewayConfig.Group.Member m : g.getMembers()) {
                ms.add(Map.of("providerId", m.getProviderId(), "model", m.getModel(),
                        "priority", m.getPriority() == null ? 0 : m.getPriority()));
            }
            gv.put("members", ms);
            out.add(gv);
        }
        return out;
    }

    // ================= 供应商 CRUD（持久化） =================

    /** 新增供应商（body: id/name/baseUrl/protocol/priority/costTier/apiKey/[proxy]/[enabled]） */
    @PostMapping("/providers")
    public ResponseEntity<?> addProvider(@RequestBody Map<String, Object> b) {
        String id = str(b.get("id"));
        String baseUrl = str(b.get("baseUrl"));
        if (id == null || id.isBlank() || baseUrl == null || baseUrl.isBlank())
            return bad("id 和 baseUrl 必填");
        return persistOr500(cfg -> {
            for (GatewayConfig.Provider p : cfg.getProviders())
                if (p.getId().equals(id)) throw new IllegalArgumentException("供应商已存在: " + id);
            GatewayConfig.Provider p = new GatewayConfig.Provider();
            p.setId(id);
            p.setName(orDefault(str(b.get("name")), id));
            p.setBaseUrl(baseUrl);
            p.setProtocol(orDefault(str(b.get("protocol")), "openai"));
            p.setPriority(intOr(b.get("priority"), 5));
            p.setDefaultCostTier(orDefault(str(b.get("costTier")), "standard"));
            p.setEnabled(boolOr(b.get("enabled"), true));
            String proxy = str(b.get("proxy"));
            if (proxy != null && !proxy.isBlank()) p.setProxy(proxy);
            String tm = str(b.get("thinkingMode"));
            if (tm != null && !tm.isBlank()) p.setThinkingMode(tm);
            GatewayConfig.Channel ch = new GatewayConfig.Channel();
            ch.setId("main");
            ch.setApiKey(orDefault(str(b.get("apiKey")), ""));
            ch.setEnabled(true);
            ch.setWeight(1);
            p.getChannels().add(ch);
            cfg.getProviders().add(p);
        });
    }

    /** 修改供应商（可改 name/baseUrl/protocol/priority/costTier/enabled/proxy/thinkingMode/apiKey） */
    @PutMapping("/providers/{id}")
    public ResponseEntity<?> updateProvider(@PathVariable("id") String id, @RequestBody Map<String, Object> b) {
        return persistOr500(cfg -> {
            GatewayConfig.Provider p = findProvider(cfg, id);
            if (b.containsKey("name")) p.setName(str(b.get("name")));
            if (b.containsKey("baseUrl")) p.setBaseUrl(str(b.get("baseUrl")));
            if (b.containsKey("protocol")) p.setProtocol(str(b.get("protocol")));
            if (b.containsKey("priority")) p.setPriority(intOr(b.get("priority"), p.getPriority()));
            if (b.containsKey("costTier")) p.setDefaultCostTier(str(b.get("costTier")));
            if (b.containsKey("enabled")) p.setEnabled(boolOr(b.get("enabled"), true));
            if (b.containsKey("proxy")) p.setProxy(str(b.get("proxy")));
            if (b.containsKey("thinkingMode")) p.setThinkingMode(str(b.get("thinkingMode")));
            if (b.containsKey("apiKey") && !p.getChannels().isEmpty())
                p.getChannels().get(0).setApiKey(str(b.get("apiKey")));
        });
    }

    /** 删除供应商（连同其渠道、路由覆盖、组成员引用） */
    @DeleteMapping("/providers/{id}")
    public ResponseEntity<?> deleteProvider(@PathVariable("id") String id) {
        ResponseEntity<?> resp = persistOr500(cfg -> {
            if (!cfg.getProviders().removeIf(p -> p.getId().equals(id)))
                throw new IllegalArgumentException("供应商不存在: " + id);
            cfg.getRoutingOverrides().removeIf(ro -> ro.getProviderId().equals(id));
            for (GatewayConfig.Group g : cfg.getGroups())
                g.getMembers().removeIf(m -> m.getProviderId().equals(id));
        });
        // 删除成功后清理运行时状态（熔断/EWMA/冷却/限流学习），防同 id 重建继承旧状态
        if (resp.getStatusCode().is2xxSuccessful()) policyService.clearRuntimeFor(id);
        return resp;
    }

    /** 供应商整体启停（持久化） */
    @PostMapping("/providers/{id}/{action}")
    public ResponseEntity<?> providerAction(@PathVariable("id") String id, @PathVariable("action") String action) {
        boolean enable = switch (action) {
            case "enable" -> true;
            case "disable" -> false;
            default -> throw new IllegalArgumentException("action 必须为 enable/disable");
        };
        return persistOr500(cfg -> findProvider(cfg, id).setEnabled(enable));
    }

    // ================= 供应商模型（static-models）CRUD =================

    /** 新增某供应商的模型（body: model 必填；priority/costTier/dailyTokenLimit/alias 可选 → 同步建路由条目） */
    @PostMapping("/providers/{pid}/models")
    public ResponseEntity<?> addModel(@PathVariable("pid") String pid, @RequestBody Map<String, Object> b) {
        String model = str(b.get("model"));
        if (model == null || model.isBlank()) return bad("model 必填");
        return persistOr500(cfg -> {
            GatewayConfig.Provider p = findProvider(cfg, pid);
            if (p.getStaticModels().contains(model))
                throw new IllegalArgumentException("模型已存在: " + model);
            p.getStaticModels().add(model);
            // 可选参数同步建路由条目
            if (b.containsKey("priority") || b.containsKey("costTier") || b.containsKey("dailyTokenLimit")
                    || b.containsKey("alias") || b.containsKey("concurrencyLimit")
                    || b.containsKey("category") || b.containsKey("contextWindow")
                    || b.containsKey("excludeFromAutoAll") || b.containsKey("codeScore")
                    || b.containsKey("userWeight") || b.containsKey("toolWeak") || b.containsKey("excludeFromProbe")) {
                GatewayConfig.RoutingOverride ro = findOrCreateOverride(cfg, pid, model);
                if (b.containsKey("priority")) ro.setPriority(intOrNull(b.get("priority")));
                if (b.containsKey("costTier")) ro.setCostTier(str(b.get("costTier")));
                if (b.containsKey("dailyTokenLimit")) ro.setDailyTokenLimit(longOrNull(b.get("dailyTokenLimit")));
                if (b.containsKey("concurrencyLimit")) ro.setConcurrencyLimit(intOrNull(b.get("concurrencyLimit")));
                if (b.containsKey("alias")) ro.setAlias(str(b.get("alias")));
                if (b.containsKey("category")) ro.setCategory(str(b.get("category")));
                if (b.containsKey("contextWindow")) ro.setContextWindow(intOrNull(b.get("contextWindow")));
                if (b.containsKey("excludeFromAutoAll")) ro.setExcludeFromAutoAll(Boolean.TRUE.equals(b.get("excludeFromAutoAll")));
                if (b.containsKey("codeScore")) ro.setCodeScore(doubleOrNull(b.get("codeScore")));
                if (b.containsKey("userWeight")) ro.setUserWeight(doubleOrNull(b.get("userWeight")));
                if (b.containsKey("toolWeak")) ro.setToolWeak(Boolean.TRUE.equals(b.get("toolWeak")) ? Boolean.TRUE : null);
                if (b.containsKey("excludeFromProbe")) ro.setExcludeFromProbe(Boolean.TRUE.equals(b.get("excludeFromProbe")) ? Boolean.TRUE : null);
            }
        });
    }

    /** 修改某供应商的模型（改名 + 更新路由条目参数）
     *  合并行场景：传入名可能是 static 真名或路由条目名，先归一到 static 真名再改。
     *  模型名可含 "/"，走 POST /update + 请求体（同 delete 原因）。 */
    @PostMapping("/providers/{pid}/models/update")
    public ResponseEntity<?> updateModelPost(@PathVariable("pid") String pid, @RequestBody Map<String, Object> b) {
        String model = str(b.get("model"));
        if (model == null || model.isBlank()) return bad("model 必填");
        return doUpdateModel(pid, model, b);
    }

    /** 旧路径（模型名不含 / 时可用；前端已统一切到 POST 版） */
    @PutMapping("/providers/{pid}/models/{model}")
    public ResponseEntity<?> updateModel(@PathVariable("pid") String pid, @PathVariable("model") String model,
                                         @RequestBody Map<String, Object> b) {
        return doUpdateModel(pid, model, b);
    }

    private ResponseEntity<?> doUpdateModel(String pid, String model, Map<String, Object> b) {
        String newModel = str(b.get("newModel"));
        return persistOr500(cfg -> {
            GatewayConfig.Provider p = findProvider(cfg, pid);
            // 归一化：路由条目名 → 其 alias（static 真名）；不是路由名则原样
            String curStatic = model;
            for (GatewayConfig.RoutingOverride ro : cfg.getRoutingOverrides()) {
                if (ro.getProviderId().equals(pid) && ro.getModel().equals(model)
                        && ro.getAlias() != null && !ro.getAlias().isBlank()
                        && p.getStaticModels().contains(ro.getAlias())) {
                    curStatic = ro.getAlias();
                    break;
                }
            }
            if (!p.getStaticModels().contains(curStatic) && !hasOverride(cfg, pid, model))
                throw new IllegalArgumentException("模型不存在: " + model);
            String target = curStatic;
            // 改名：static-models + 路由条目 + 分组成员 三处同步。
            // alias 防脱钩：请求名（ro.model）改了，上游真名（alias）保持不变——
            // 上游模型名区分大小写（AMD），改名时若不同步保持 alias 会把上游真名一起改坏（model_not_found）。
            boolean renameAliasIfNeeded = false;
            if (newModel != null && !newModel.isBlank() && !newModel.equals(target)) {
                if (p.getStaticModels().contains(newModel))
                    throw new IllegalArgumentException("新模型名已存在: " + newModel);
                if (p.getStaticModels().remove(curStatic))
                    p.getStaticModels().add(newModel);
                for (GatewayConfig.RoutingOverride ro : cfg.getRoutingOverrides()) {
                    if (ro.getProviderId().equals(pid) && ro.getModel().equals(curStatic)) {
                        ro.setModel(newModel);
                        // 原本 alias=旧名（请求名与真名相同）→ 新名若与真名不同步，上游会 404。
                        // 若 alias 原本指向旧名本身，跟随改成新名；否则（真名不同）保留原 alias。
                        if (curStatic.equals(ro.getAlias())) renameAliasIfNeeded = true;
                    }
                }
                for (GatewayConfig.Group g : cfg.getGroups()) {
                    for (GatewayConfig.Group.Member m : g.getMembers()) {
                        if (m.getProviderId().equals(pid) && m.getModel().equals(curStatic)) m.setModel(newModel);
                    }
                }
                target = newModel;  // 后续参数写到新名上
            }
            if (b.containsKey("priority") || b.containsKey("costTier") || b.containsKey("dailyTokenLimit")
                    || b.containsKey("alias") || b.containsKey("concurrencyLimit") || b.containsKey("category")
                    || b.containsKey("contextWindow") || b.containsKey("excludeFromAutoAll")
                    || b.containsKey("codeScore") || b.containsKey("userWeight") || b.containsKey("toolWeak")
                    || b.containsKey("excludeFromProbe")
                    || renameAliasIfNeeded) {
                GatewayConfig.RoutingOverride ro = findOrCreateOverride(cfg, pid, target);
                if (b.containsKey("priority")) ro.setPriority(intOrNull(b.get("priority")));
                if (b.containsKey("costTier")) ro.setCostTier(str(b.get("costTier")));
                if (b.containsKey("dailyTokenLimit")) ro.setDailyTokenLimit(longOrNull(b.get("dailyTokenLimit")));
                if (b.containsKey("concurrencyLimit")) ro.setConcurrencyLimit(intOrNull(b.get("concurrencyLimit")));
                if (b.containsKey("alias")) ro.setAlias(str(b.get("alias")));
                if (b.containsKey("category")) ro.setCategory(str(b.get("category")));
                if (b.containsKey("contextWindow")) ro.setContextWindow(intOrNull(b.get("contextWindow")));
                if (b.containsKey("excludeFromAutoAll")) ro.setExcludeFromAutoAll(Boolean.TRUE.equals(b.get("excludeFromAutoAll")));
                if (b.containsKey("codeScore")) ro.setCodeScore(doubleOrNull(b.get("codeScore")));
                if (b.containsKey("userWeight")) ro.setUserWeight(doubleOrNull(b.get("userWeight")));
                if (b.containsKey("toolWeak")) ro.setToolWeak(Boolean.TRUE.equals(b.get("toolWeak")) ? Boolean.TRUE : null);
                if (b.containsKey("excludeFromProbe")) ro.setExcludeFromProbe(Boolean.TRUE.equals(b.get("excludeFromProbe")) ? Boolean.TRUE : null);
                // 改名时 alias 原指向旧请求名（请求名=真名场景）→ 跟随新名，保持"请求名=真名"语义。
                // 独立判断（原先挂在 else-if 链尾，前端全量提交时永远走不到，改名跟随形同虚设）；
                // 显式带了 alias 字段的以请求为准
                if (renameAliasIfNeeded && !b.containsKey("alias")) ro.setAlias(newModel);
            }
        });
    }

    private static boolean hasOverride(GatewayConfig cfg, String pid, String model) {
        for (GatewayConfig.RoutingOverride ro : cfg.getRoutingOverrides())
            if (ro.getProviderId().equals(pid) && ro.getModel().equals(model)) return true;
        return false;
    }

    /** 删除某供应商的模型（static-models + 路由条目 + 分组成员引用）
     *  合并行场景（overview 把 override 条目与被吞并的 static 条目合成一行）：
     *  传入名可能是 static 真名或路由条目名，任一命中即删，两边引用全部清掉。
     *  模型名可含 "/"（如 z-ai/glm-5.3-free），不能安全放 URL 路径（%2F 被 Tomcat 拒 400），
     *  故走 POST /delete + 请求体。 */
    @PostMapping("/providers/{pid}/models/delete")
    public ResponseEntity<?> deleteModelPost(@PathVariable("pid") String pid, @RequestBody Map<String, Object> b) {
        String model = str(b.get("model"));
        if (model == null || model.isBlank()) return bad("model 必填");
        return doDeleteModel(pid, model);
    }

    /** 旧路径（模型名不含 / 时可用；前端已统一切到 POST 版） */
    @DeleteMapping("/providers/{pid}/models/{model}")
    public ResponseEntity<?> deleteModel(@PathVariable("pid") String pid, @PathVariable("model") String model) {
        return doDeleteModel(pid, model);
    }

    private ResponseEntity<?> doDeleteModel(String pid, String model) {
        // 记录被删的模型名（含 alias 反查出的多个名字），persist 成功后清理对应运行时状态
        final java.util.Set<String> toClear = new java.util.LinkedHashSet<>();
        ResponseEntity<?> resp = persistOr500(cfg -> {
            GatewayConfig.Provider p = findProvider(cfg, pid);
            // 反查：若传入的是路由条目名，找到它吞并的 static 真名（alias）；反之亦然
            java.util.Set<String> names = new java.util.LinkedHashSet<>();
            names.add(model);
            for (GatewayConfig.RoutingOverride ro : cfg.getRoutingOverrides()) {
                if (ro.getProviderId().equals(pid) && model.equals(ro.getAlias())) names.add(ro.getModel());
                if (ro.getProviderId().equals(pid) && ro.getModel().equals(model)
                        && ro.getAlias() != null) names.add(ro.getAlias());
            }
            toClear.addAll(names);
            boolean anyStatic = false;
            for (String n : names) anyStatic |= p.getStaticModels().remove(n);
            boolean anyOverride = cfg.getRoutingOverrides().removeIf(ro ->
                    ro.getProviderId().equals(pid) && names.contains(ro.getModel()));
            if (!anyStatic && !anyOverride)
                throw new IllegalArgumentException("模型不存在: " + model);
            for (GatewayConfig.Group g : cfg.getGroups())
                g.getMembers().removeIf(m -> m.getProviderId().equals(pid) && names.contains(m.getModel()));
        });
        if (resp.getStatusCode().is2xxSuccessful())
            for (String n : toClear) policyService.clearEntryFor(pid, n);
        return resp;
    }

    /** 模型级启用/停用（body: model + enabled）。
     *  合并行兼容：传入名可能是路由条目请求名（精确命中），也可能是 static 真名（alias 反查命中）；
     *  都没有就为这个名字建一条只带 enabled 的条目。启用时把 enabled 清回 null（YAML 保持干净）。 */
    @PostMapping("/providers/{pid}/models/set-enabled")
    public ResponseEntity<?> setModelEnabled(@PathVariable("pid") String pid, @RequestBody Map<String, Object> b) {
        String model = str(b.get("model"));
        if (model == null || model.isBlank()) return bad("model 必填");
        boolean enable = boolOr(b.get("enabled"), true);
        return persistOr500(cfg -> {
            GatewayConfig.Provider p = findProvider(cfg, pid);
            GatewayConfig.RoutingOverride ro = null;
            for (GatewayConfig.RoutingOverride x : cfg.getRoutingOverrides()) {
                if (!x.getProviderId().equals(pid)) continue;
                if (model.equals(x.getModel())) { ro = x; break; }
                if (model.equals(x.getAlias()) && ro == null) ro = x;   // alias 反查兜底，精确命中优先
            }
            if (ro == null) {
                if (!p.getStaticModels().contains(model))
                    throw new IllegalArgumentException("模型不存在: " + model);
                ro = findOrCreateOverride(cfg, pid, model);
            }
            ro.setEnabled(enable ? null : Boolean.FALSE);
        });
    }

    private static GatewayConfig.RoutingOverride findOrCreateOverride(GatewayConfig cfg, String pid, String model) {
        for (GatewayConfig.RoutingOverride r : cfg.getRoutingOverrides()) {
            if (r.getProviderId().equals(pid) && r.getModel().equals(model)) return r;
        }
        GatewayConfig.RoutingOverride ro = new GatewayConfig.RoutingOverride();
        ro.setProviderId(pid);
        ro.setModel(model);
        cfg.getRoutingOverrides().add(ro);
        return ro;
    }

    // ================= 渠道（Key）CRUD =================

    /** 新增渠道（body: apiKey/[weight]/[enabled]） */
    @PostMapping("/providers/{pid}/channels")
    public ResponseEntity<?> addChannel(@PathVariable("pid") String pid, @RequestBody Map<String, Object> b) {
        String apiKey = str(b.get("apiKey"));
        if (apiKey == null || apiKey.isBlank()) return bad("apiKey 必填");
        return persistOr500(cfg -> {
            GatewayConfig.Provider p = findProvider(cfg, pid);
            String chId = orDefault(str(b.get("id")), "ch" + (p.getChannels().size() + 1));
            for (GatewayConfig.Channel c : p.getChannels())
                if (c.getId().equals(chId)) throw new IllegalArgumentException("渠道 id 重复: " + chId);
            GatewayConfig.Channel ch = new GatewayConfig.Channel();
            ch.setId(chId);
            ch.setApiKey(apiKey);
            ch.setWeight(intOr(b.get("weight"), 1));
            ch.setEnabled(boolOr(b.get("enabled"), true));
            p.getChannels().add(ch);
        });
    }

    /** 修改渠道（apiKey/weight/enabled） */
    @PutMapping("/providers/{pid}/channels/{cid}")
    public ResponseEntity<?> updateChannel(@PathVariable("pid") String pid, @PathVariable("cid") String cid,
                                           @RequestBody Map<String, Object> b) {
        return persistOr500(cfg -> {
            GatewayConfig.Channel ch = findChannel(cfg, pid, cid);
            if (b.containsKey("apiKey")) ch.setApiKey(str(b.get("apiKey")));
            if (b.containsKey("weight")) ch.setWeight(intOr(b.get("weight"), ch.getWeight()));
            if (b.containsKey("enabled")) ch.setEnabled(boolOr(b.get("enabled"), true));
        });
    }

    /** 删除渠道 */
    @DeleteMapping("/providers/{pid}/channels/{cid}")
    public ResponseEntity<?> deleteChannel(@PathVariable("pid") String pid, @PathVariable("cid") String cid) {
        return persistOr500(cfg -> {
            GatewayConfig.Provider p = findProvider(cfg, pid);
            if (p.getChannels().size() <= 1)
                throw new IllegalArgumentException("最后一个渠道不能删（供应商至少保留一个 Key）");
            if (!p.getChannels().removeIf(c -> c.getId().equals(cid)))
                throw new IllegalArgumentException("渠道不存在: " + cid);
        });
    }

    /** 渠道启停（持久化） */
    @PostMapping("/channels/{pid}/{cid}/{action}")
    public ResponseEntity<?> channelAction(@PathVariable("pid") String pid, @PathVariable("cid") String cid,
                                           @PathVariable("action") String action) {
        boolean enable = switch (action) {
            case "enable" -> true;
            case "disable" -> false;
            default -> throw new IllegalArgumentException("action 必须为 enable/disable");
        };
        return persistOr500(cfg -> findChannel(cfg, pid, cid).setEnabled(enable));
    }

    // ================= 模型路由条目（routing-overrides）CRUD =================

    /** 新增/修改某供应商某模型的路由条目（覆盖优先级/费用档/日预算/alias） */
    @PostMapping("/routing")
    public ResponseEntity<?> upsertRouting(@RequestBody Map<String, Object> b) {
        String pid = str(b.get("providerId"));
        String model = str(b.get("model"));
        if (pid == null || model == null || pid.isBlank() || model.isBlank())
            return bad("providerId 和 model 必填");
        return persistOr500(cfg -> {
            findProvider(cfg, pid); // 校验存在
            GatewayConfig.RoutingOverride ro = null;
            for (GatewayConfig.RoutingOverride r : cfg.getRoutingOverrides()) {
                if (r.getProviderId().equals(pid) && r.getModel().equals(model)) { ro = r; break; }
            }
            if (ro == null) {
                ro = new GatewayConfig.RoutingOverride();
                ro.setProviderId(pid);
                ro.setModel(model);
                cfg.getRoutingOverrides().add(ro);
            }
            if (b.containsKey("priority")) ro.setPriority(intOrNull(b.get("priority")));
            if (b.containsKey("costTier")) ro.setCostTier(str(b.get("costTier")));
            if (b.containsKey("dailyTokenLimit")) ro.setDailyTokenLimit(longOrNull(b.get("dailyTokenLimit")));
            if (b.containsKey("concurrencyLimit")) ro.setConcurrencyLimit(intOrNull(b.get("concurrencyLimit")));
            if (b.containsKey("alias")) ro.setAlias(str(b.get("alias")));
            if (b.containsKey("category")) ro.setCategory(str(b.get("category")));
            if (b.containsKey("contextWindow")) ro.setContextWindow(intOrNull(b.get("contextWindow")));
            if (b.containsKey("excludeFromAutoAll")) ro.setExcludeFromAutoAll(Boolean.TRUE.equals(b.get("excludeFromAutoAll")));
            if (b.containsKey("codeScore")) ro.setCodeScore(doubleOrNull(b.get("codeScore")));
            if (b.containsKey("userWeight")) ro.setUserWeight(doubleOrNull(b.get("userWeight")));
            if (b.containsKey("toolWeak")) ro.setToolWeak(Boolean.TRUE.equals(b.get("toolWeak")) ? Boolean.TRUE : null);
            if (b.containsKey("excludeFromProbe")) ro.setExcludeFromProbe(Boolean.TRUE.equals(b.get("excludeFromProbe")) ? Boolean.TRUE : null);
        });
    }

    /** 删除路由条目（模型名可含 /，走 POST + 请求体） */
    @PostMapping("/routing/delete")
    public ResponseEntity<?> deleteRoutingPost(@RequestBody Map<String, Object> b) {
        String pid = str(b.get("providerId"));
        String model = str(b.get("model"));
        if (pid == null || model == null || pid.isBlank() || model.isBlank())
            return bad("providerId 和 model 必填");
        return doDeleteRouting(pid, model);
    }

    /** 旧路径（模型名不含 / 时可用） */
    @DeleteMapping("/routing/{pid}/{model}")
    public ResponseEntity<?> deleteRouting(@PathVariable("pid") String pid, @PathVariable("model") String model) {
        return doDeleteRouting(pid, model);
    }

    private ResponseEntity<?> doDeleteRouting(String pid, String model) {
        return persistOr500(cfg -> {
            if (!cfg.getRoutingOverrides()
                    .removeIf(ro -> ro.getProviderId().equals(pid) && ro.getModel().equals(model)))
                throw new IllegalArgumentException("路由条目不存在: " + pid + "/" + model);
        });
    }

    // ================= 虚拟模型分组 CRUD =================

    @PostMapping("/groups")
    public ResponseEntity<?> upsertGroup(@RequestBody Map<String, Object> b) {
        String name = str(b.get("name"));
        if (name == null || name.isBlank()) return bad("name 必填");
        Object membersObj = b.get("members");
        boolean autoAll = boolOr(b.get("autoAll"), false);
        if (!autoAll && (!(membersObj instanceof List<?> membersList) || membersList.isEmpty()))
            return bad("members 不能为空（或开启 autoAll 自动包含全部模型）");
        return persistOr500(cfg -> {
            GatewayConfig.Group g = null;
            for (GatewayConfig.Group x : cfg.getGroups()) {
                if (x.getName().equals(name)) { g = x; break; }
            }
            if (g == null) {
                g = new GatewayConfig.Group();
                g.setName(name);
                cfg.getGroups().add(g);
            }
            g.setAutoAll(autoAll);
            if (!autoAll) {
                g.getMembers().clear();
                for (Object o : (List<?>) membersObj) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> m = (Map<String, Object>) o;
                    String pid = str(m.get("providerId"));
                    String model = str(m.get("model"));
                    if (pid == null || model == null) throw new IllegalArgumentException("组成员缺 providerId/model");
                    findProvider(cfg, pid);
                    GatewayConfig.Group.Member gm = new GatewayConfig.Group.Member();
                    gm.setProviderId(pid);
                    gm.setModel(model);
                    // 用 intOrNull 而非 intOr(..., null)：缺省须保持 null 以回退到 provider/override 优先级，
                    // 若误存 0 会被 effectivePriority 当作"永远最高优先级"
                    gm.setPriority(intOrNull(m.get("priority")));
                    g.getMembers().add(gm);
                }
            } else {
                g.getMembers().clear();   // 自动组成员动态展开，无需手工维护
            }
        });
    }

    @DeleteMapping("/groups/{name}")
    public ResponseEntity<?> deleteGroup(@PathVariable("name") String name) {
        return persistOr500(cfg -> {
            if (!cfg.getGroups().removeIf(g -> g.getName().equals(name)))
                throw new IllegalArgumentException("分组不存在: " + name);
        });
    }

    // ================= 用量统计 =================

    /**
     * 每供应商×每模型 请求次数与 token 消耗聚合。
     * ?from=YYYY-MM-DD&to=YYYY-MM-DD 日历日区间（闭区间，单填一个=单边；都不填=近30天）。
     * ?providers=&models=&clients= 逗号分隔多值过滤（页面多选下拉）：分别作用于
     * 供应商、真实模型、来源工具；来源工具勾选后总量视图改看对应工具桶（口径与不勾时的总额桶一致）。
     * 所有子视图（按工具/按天/失败榜）与主窗口同一日期+过滤口径——此前"近N天"用 86400 秒滚动窗口
     * 与日历日不对齐（21:33 查"近7天"会把昨天 21:33 之后的失败计入、而选"昨天"单日看不到），是数据对不上的根因。
     */
    @GetMapping("/usage")
    public Map<String, Object> usage(@RequestParam(value = "from", required = false) String from,
                                     @RequestParam(value = "to", required = false) String to,
                                     @RequestParam(value = "day", required = false) String day,
                                     @RequestParam(value = "days", required = false) Integer days,
                                     @RequestParam(value = "providers", required = false) String providers,
                                     @RequestParam(value = "models", required = false) String models,
                                     @RequestParam(value = "clients", required = false) String clients) {
        try {
            // 兼容旧参数：day= 精确单日（from=to=day）；days=N 近 N 天（to=今天，from=今天往前 N-1 天）
            if ((from == null || from.isBlank()) && (to == null || to.isBlank())) {
                if (day != null && !day.isBlank()) {
                    from = to = day;
                } else {
                    int d = days == null ? 30 : Math.max(1, Math.min(365, days));
                    to = java.time.LocalDate.now().toString();
                    from = java.time.LocalDate.now().minusDays(d - 1L).toString();
                }
            }
            // 单边补默认（只填 from=从那天到今天；只填 to=从头到那天）；from > to 交换，防手填反了查空
            boolean hasFrom = from != null && !from.isBlank(), hasTo = to != null && !to.isBlank();
            if (hasFrom && !hasTo) to = java.time.LocalDate.now().toString();
            if (!hasFrom && hasTo) from = "2000-01-01";
            if (from.compareTo(to) > 0) { String t = from; from = to; to = t; }
            List<String> pv = csv(providers), md = csv(models), cl = csv(clients);
            List<Map<String, Object>> rows = statsDao.usageByProviderModel(from, to, pv, md, cl);
            List<Map<String, Object>> byDay = statsDao.usageByDay(from, to, pv, md, cl);
            // #16 日趋势补失败率：按天从 request_log 取 status!='OK' 计数，并入 byDay（无失败则 0）
            Map<String, Long> failByDay = new LinkedHashMap<>();
            for (Map<String, Object> fd : statsDao.failuresByDay(from, to, pv, md, cl)) {
                failByDay.put(String.valueOf(fd.get("day")), ((Number) fd.get("failures")).longValue());
            }
            for (Map<String, Object> b : byDay) {
                long req = ((Number) b.getOrDefault("requests", 0L)).longValue();
                long fails = failByDay.getOrDefault(String.valueOf(b.get("day")), 0L);
                b.put("failures", fails);
                b.put("failRate", req + fails > 0 ? Math.round(fails * 1000.0 / (req + fails)) / 10.0 : 0.0);
            }
            List<Map<String, Object>> byClient = statsService.clientUsageView(from, to, pv, md, cl);
            return Map.of("from", from, "to", to, "rows", rows, "byDay", byDay, "byClient", byClient,
                    "failByClient", statsDao.failuresByClient(from, to, pv, md, cl),
                    "failByModel", statsDao.failuresByProviderModel(from, to, pv, md, cl));
        } catch (Exception e) {
            return Map.of("rows", List.of(), "byDay", List.of(), "error", e.getMessage());
        }
    }

    // ================= 其他操作 =================

    /**
     * 模型连通性测试：直连指定供应商×模型（绕过路由/熔断），发一句用户输入，
     * 返回模型回复文本与耗时。失败返回 HTTP 502 + 上游错误。
     */
    @PostMapping("/test-model")
    public ResponseEntity<?> testModel(@RequestBody Map<String, Object> b) {
        String pid = str(b.get("providerId"));
        String model = str(b.get("model"));
        String prompt = orDefault(str(b.get("prompt")), "回复：连通测试 OK");
        if (pid == null || pid.isBlank() || model == null || model.isBlank())
            return bad("providerId 和 model 必填");

        ConfigService.Snapshot snap = configService.snapshot();
        GatewayConfig.Provider p = snap.providersById.get(pid);
        if (p == null) return bad("供应商不存在: " + pid);
        // 真实模型名：routing-override.alias 优先
        GatewayConfig.RoutingOverride ro = snap.overrideOf(pid, model);
        String realModel = ro != null && ro.getAlias() != null && !ro.getAlias().isBlank() ? ro.getAlias() : model;

        Map<String, Object> req = new LinkedHashMap<>();
        req.put("model", realModel);
        req.put("stream", false);
        req.put("max_tokens", 512);
        req.put("messages", List.of(Map.of("role", "user", "content", prompt)));

        // 遍历所有 enabled 渠道逐个尝试：某个 key 额度耗尽/失败就换下一把 key，
        // 与主转发路径的 failover 一致，避免"第一把 key 撞额度"就误报整家不通。
        List<GatewayConfig.Channel> enabled = p.getChannels().stream()
                .filter(GatewayConfig.Channel::isEnabled).toList();
        if (enabled.isEmpty()) return bad("该供应商没有启用的渠道");

        StringBuilder errs = new StringBuilder();
        for (GatewayConfig.Channel ch : enabled) {
            PolicyService.Candidate cand = new PolicyService.Candidate(pid, model, ch.getId(), 1, null);
            long start = System.currentTimeMillis();
            ForwardEngine.ForwardResult r = forwardEngine.execute(req, cand, false,
                    System.nanoTime() + 60_000_000_000L);
            long cost = System.currentTimeMillis() - start;

            if (r.kind == ForwardEngine.ForwardResult.Kind.SUCCESS) {
                // 非流式聚合结果（OpenAI 格式）→ 抽回复文本
                String content = extractContent(r.parsedBody);
                Map<String, Object> usage = Json.asMap(r.parsedBody.get("usage"));
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("ok", true);
                out.put("providerId", pid);
                out.put("model", model);
                out.put("channelId", ch.getId());
                out.put("realModel", realModel);
                out.put("content", content);
                out.put("costMs", cost);
                out.put("tokensIn", usage == null ? null : Json.longVal(usage.get("prompt_tokens")));
                out.put("tokensOut", usage == null ? null : Json.longVal(usage.get("completion_tokens")));
                return ResponseEntity.ok(out);
            }

            // 该渠道失败，记录并换下一把 key
            String errSummary = r.body == null ? r.error
                    : r.error + " | " + abbreviate(r.body.replaceAll("[\\r\\n]", " "), 200);
            if (!errs.isEmpty()) errs.append("；");
            errs.append(ch.getId()).append(": ").append(errSummary);
        }

        // 所有渠道都失败
        return ResponseEntity.status(502).body(Map.of(
                "ok", false, "providerId", pid, "model", model,
                "triedChannels", enabled.stream().map(GatewayConfig.Channel::getId).toList(),
                "error", errs.toString()));
    }

    /** 从 OpenAI 格式响应抽 assistant 回复文本（兼容 content 为数组的多段结构） */
    private static String extractContent(Map<String, Object> resp) {
        try {
            List<Object> choices = Json.asList(resp.get("choices"));
            if (choices == null || choices.isEmpty()) return "(无 choices)";
            Map<String, Object> first = Json.asMap(choices.get(0));
            Map<String, Object> message = Json.asMap(first.get("message"));
            Object content = message == null ? null : message.get("content");
            if (content instanceof String s) return s;
            if (content instanceof List<?> parts) {
                StringBuilder sb = new StringBuilder();
                for (Object o : parts) {
                    Map<String, Object> pm = Json.asMap(o);
                    if (pm != null && pm.get("text") instanceof String t) sb.append(t);
                }
                return sb.toString();
            }
            return "(空回复)";
        } catch (Exception e) {
            return "(解析回复失败: " + e.getMessage() + ")";
        }
    }

    private static String abbreviate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }

    /**
     * 查询供应商上游真实模型清单：实时拉该供应商 /v1/models（用其自身渠道鉴权），
     * 返回列表并标记每个是否已在本地配置（configured）。不污染目录缓存、不改配置。
     * 用于核对配置的模型名/别名是否正确。
     */
    @GetMapping("/providers/{pid}/upstream-models")
    public ResponseEntity<?> upstreamModels(@PathVariable("pid") String pid) {
        ConfigService.Snapshot snap = configService.snapshot();
        GatewayConfig.Provider p = snap.providersById.get(pid);
        if (p == null) return bad("供应商不存在: " + pid);

        long start = System.currentTimeMillis();
        List<String> upstream;
        Map<String, Map<String, String>> pricingByModel;   // modelId(lower) -> {prompt, completion}（美元/token）
        try {
            String raw = catalogService.fetchUpstreamModelsRaw(p);
            upstream = new ArrayList<>();
            pricingByModel = new java.util.HashMap<>();
            try {
                Map<String, Object> parsed = Json.parseObject(raw);
                List<Object> data = Json.asList(parsed.get("data"));
                if (data != null) {
                    for (Object o : data) {
                        Map<String, Object> m = Json.asMap(o);
                        if (m == null || m.get("id") == null) continue;
                        String id = String.valueOf(m.get("id"));
                        upstream.add(id);
                        Map<String, Object> pr = Json.asMap(m.get("pricing"));
                        if (pr != null) {
                            Map<String, String> pv = new LinkedHashMap<>();
                            Object prompt = pr.get("prompt");
                            Object completion = pr.get("completion");
                            if (prompt != null) pv.put("prompt", String.valueOf(prompt));
                            if (completion != null) pv.put("completion", String.valueOf(completion));
                            if (!pv.isEmpty()) pricingByModel.put(id.toLowerCase(), pv);
                        }
                    }
                }
            } catch (Exception ignore) {
                // 原始体不是标准结构（各家差异）：退回纯 id 解析
                upstream = catalogService.fetchUpstreamModelsNow(p);
                pricingByModel = new java.util.HashMap<>();
            }
        } catch (Exception e) {
            // 限流（AMD 等免费上游常见 ip_rate_limit_exceeded）与普通失败分开提示：
            // 限流是暂时的，等 30s 重试即可，不代表配置有错
            CatalogService.FetchResult fr = e instanceof CatalogService.FetchException fe ? fe.result : null;
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ok", false);
            body.put("providerId", pid);
            if (fr != null && fr.status() == 429) {
                body.put("rateLimited", true);
                body.put("error", "上游限流 (HTTP 429)：该供应商对同一 IP 的模型清单请求有频率限制，等 30~60 秒后点\"查询\"重试即可。这不影响模型调用（聊天与清单是不同的限流桶）。");
            } else {
                body.put("rateLimited", false);
                body.put("error", "拉取失败: " + e.getMessage());
                body.put("hint", "上游不支持 /v1/models 或鉴权/网络问题");
            }
            return ResponseEntity.status(502).body(body);
        }
        long costMs = System.currentTimeMillis() - start;

        // 本地已配置集合（大小写不敏感）：模型名 + alias 都算
        java.util.Set<String> configuredLower = new java.util.HashSet<>();
        for (String m : p.getStaticModels()) configuredLower.add(m.toLowerCase());
        for (GatewayConfig.RoutingOverride ro : snap.config.getRoutingOverrides()) {
            if (ro.getProviderId().equals(pid)) {
                if (ro.getModel() != null) configuredLower.add(ro.getModel().toLowerCase());
                if (ro.getAlias() != null && !ro.getAlias().isBlank())
                    configuredLower.add(ro.getAlias().toLowerCase());
            }
        }
        List<Map<String, Object>> models = new ArrayList<>();
        java.util.Set<String> upstreamLower = new java.util.HashSet<>();
        for (String m : upstream) {
            upstreamLower.add(m.toLowerCase());
            Map<String, Object> mv = new LinkedHashMap<>();
            mv.put("model", m);
            mv.put("configured", configuredLower.contains(m.toLowerCase()));
            // 价格信息（美元/token 字符串；有 pricing 字段的上游才有）
            Map<String, String> pr = pricingByModel.get(m.toLowerCase());
            if (pr != null) {
                mv.put("pricePrompt", pr.get("prompt"));
                mv.put("priceCompletion", pr.get("completion"));
            }
            models.add(mv);
        }
        // 本地配置了但上游清单找不到的（拼错/已下架/alias 反了），需人工核对
        List<String> missing = new ArrayList<>();
        for (String m : p.getStaticModels()) {
            String al = aliasOf(snap, pid, m);
            boolean directHit = upstreamLower.contains(m.toLowerCase());
            boolean aliasHit = al != null && upstreamLower.contains(al.toLowerCase());
            if (!directHit && !aliasHit) missing.add(al == null ? m : m + " → alias:" + al);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("providerId", pid);
        out.put("count", models.size());
        out.put("costMs", costMs);
        out.put("models", models);
        out.put("missing", missing);
        // 顺带查账户余额（数据驱动：未配的供应商 fetchBalance 直接返回"未配置"；失败不阻断清单响应）
        Map<String, Object> bal = new LinkedHashMap<>();
        CatalogService.BalanceResult br = catalogService.fetchBalance(p);
        bal.put("configured", p.getBalance() != null);
        bal.put("ok", br.ok());
        if (br.ok()) {
            bal.put("amount", br.amount());
            bal.put("currency", br.currency());
        } else {
            bal.put("error", br.error());
        }
        out.put("balance", bal);
        return ResponseEntity.ok(out);
    }

    private static String aliasOf(ConfigService.Snapshot snap, String pid, String model) {
        GatewayConfig.RoutingOverride ro = snap.overrideOf(pid, model);
        return ro == null || ro.getAlias() == null || ro.getAlias().isBlank() ? null : ro.getAlias();
    }

    @PostMapping("/budget/reset")
    public Map<String, Object> resetBudget() {
        policyService.resetBudget();
        return Map.of("ok", true);
    }

    /** 自动发现上游模型清单开关（默认关：模型清单只信手工配置的 static-models） */
    @GetMapping("/catalog-discovery")
    public Map<String, Object> getCatalogDiscovery() {
        return Map.of("enabled", configService.snapshot().config.getDefaults().isCatalogDiscovery());
    }

    @PutMapping("/catalog-discovery")
    public ResponseEntity<?> setCatalogDiscovery(@RequestBody Map<String, Object> body) {
        boolean en = boolOr(body.get("enabled"), false);
        try {
            configService.persist(cfg -> cfg.getDefaults().setCatalogDiscovery(en));
            catalogService.refreshAll();
            return ResponseEntity.ok(Map.of("ok", true, "enabled", en,
                    "catalogSize", catalogService.fullCatalog().size()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/catalog/refresh")
    public Map<String, Object> refreshCatalog() {
        catalogService.refreshAll();
        return Map.of("ok", true, "catalogSize", catalogService.fullCatalog().size());
    }

    /** 速带分摊抽签（loadShare）：开关 + 抽签带宽 + 慢门槛 + 速度并列带倍率 + 慢性限流三参数，
     *  界面可改、落盘持久、实时生效（rank() 每次读快照 defaults，persist 换快照后下一请求即用新值）。 */
    @GetMapping("/load-share")
    public Map<String, Object> getLoadShare() {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("enabled", d.isLoadShare());
        out.put("bandSize", d.getShareBandSize());
        out.put("slowTtftMs", d.getShareSlowTtftMs());
        out.put("speedTieRatio", d.getShareSpeedTieRatio());
        out.put("providerSeatCap", d.getShareProviderSeatCap());
        out.put("providerCapPct", d.getShareProviderCapPct());
        out.put("chronicWindowMinutes", d.getChronicRateWindowMinutes());
        out.put("chronicMinSamples", d.getChronicRateMinSamples());
        out.put("chronicSeatBlockRatio", d.getChronicSeatBlockRatio());
        // 实测 TTFT 统计 + 推荐慢门槛（界面展示，改不改由用户点按钮决定，不自动落盘）
        out.put("ttft", policyService.ttftStats());
        return out;
    }

    @PutMapping("/load-share")
    public ResponseEntity<?> setLoadShare(@RequestBody Map<String, Object> body) {
        Boolean en = body.containsKey("enabled") ? boolOr(body.get("enabled"), false) : null;
        Integer band = intOrNull(body.get("bandSize"));
        Integer slow = intOrNull(body.get("slowTtftMs"));
        Double tie = doubleOrNull(body.get("speedTieRatio"));
        Integer seatCap = intOrNull(body.get("providerSeatCap"));
        Double capPct = doubleOrNull(body.get("providerCapPct"));
        Integer cWin = intOrNull(body.get("chronicWindowMinutes"));
        Integer cMin = intOrNull(body.get("chronicMinSamples"));
        Double cRatio = doubleOrNull(body.get("chronicSeatBlockRatio"));
        if (band != null && (band < 2 || band > 10)) return bad("bandSize 需在 2~10");
        if (slow != null && (slow < 1000 || slow > 60000)) return bad("slowTtftMs 需在 1000~60000");
        if (tie != null && (tie < 1.0 || tie > 20.0)) return bad("speedTieRatio 需在 1.0~20.0（1=速度一有差就分份额）");
        if (seatCap != null && (seatCap < 0 || seatCap > 10)) return bad("providerSeatCap 需在 0~10（0=一家不限席）");
        // 0 是"关闭封顶"的显式动作，必须能存住，所以单独放行；非 0 时低于 10% 无意义（一家连一成都接不住）
        if (capPct != null && !(capPct == 0 || (capPct >= 0.1 && capPct <= 1.0)))
            return bad("providerCapPct 需为 0（不限）或 0.1~1.0");
        if (cWin != null && (cWin < 0 || cWin > 1440)) return bad("chronicWindowMinutes 需在 0~1440（0=关闭慢性限流层）");
        if (cMin != null && (cMin < 3 || cMin > 100)) return bad("chronicMinSamples 需在 3~100");
        if (cRatio != null && (cRatio < 0.05 || cRatio > 1.0)) return bad("chronicSeatBlockRatio 需在 0.05~1.0");
        if (en == null && band == null && slow == null && tie == null && seatCap == null && capPct == null
                && cWin == null && cMin == null && cRatio == null)
            return bad("enabled/bandSize/slowTtftMs/speedTieRatio/providerSeatCap/providerCapPct/慢性限流参数 至少给一个");
        try {
            configService.persist(cfg -> {
                GatewayConfig.Defaults d = cfg.getDefaults();
                if (en != null) d.setLoadShare(en);
                if (band != null) d.setShareBandSize(band);
                if (slow != null) d.setShareSlowTtftMs(slow);
                if (tie != null) d.setShareSpeedTieRatio(tie);
                if (seatCap != null) d.setShareProviderSeatCap(seatCap);
                if (capPct != null) d.setShareProviderCapPct(capPct);
                if (cWin != null) d.setChronicRateWindowMinutes(cWin);
                if (cMin != null) d.setChronicRateMinSamples(cMin);
                if (cRatio != null) d.setChronicSeatBlockRatio(cRatio);
            });
            GatewayConfig.Defaults nd = configService.snapshot().config.getDefaults();
            Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("ok", true);
            out.put("enabled", nd.isLoadShare());
            out.put("bandSize", nd.getShareBandSize());
            out.put("slowTtftMs", nd.getShareSlowTtftMs());
            out.put("speedTieRatio", nd.getShareSpeedTieRatio());
            out.put("providerSeatCap", nd.getShareProviderSeatCap());
            out.put("providerCapPct", nd.getShareProviderCapPct());
            out.put("chronicWindowMinutes", nd.getChronicRateWindowMinutes());
            out.put("chronicMinSamples", nd.getChronicRateMinSamples());
            out.put("chronicSeatBlockRatio", nd.getChronicSeatBlockRatio());
            return ResponseEntity.ok(out);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    // ================= 评分与分摊（决策留痕） =================

    /**
     * 评分沙盘：用当前运行态（健康/冷却/限流/样本）重放一次路由决策，返回每个候选的
     * 综合分、因子分解、抽签席与应得份额、以及被门控直接剔除的候选和原因。
     * 预览不改运行态：不占半开探测槽、不写留痕库。
     */
    @PostMapping("/rank-preview")
    public ResponseEntity<?> rankPreview(@RequestBody Map<String, Object> body) {
        String model = Json.str(body.get("model"));
        if (model == null || model.isBlank()) return bad("model 必填（组名 / provider/model 点名 / 原始模型名都行）");
        List<PolicyService.Candidate> candidates = routerService.resolve(model);
        if (candidates.isEmpty()) {
            return ResponseEntity.ok(Map.of("model", model, "candidates", 0, "rows", List.of(),
                    "filtered", List.of(), "note", "该 model 解析不出任何候选：名字不存在或所属供应商/渠道全被停用"));
        }
        String strategy = body.get("strategy") == null
                ? configService.snapshot().config.getStrategy() : Json.str(body.get("strategy"));
        PolicyService.RankTrace t = new PolicyService.RankTrace();
        t.preview = true;
        t.requestedModel = model;
        t.client = Json.str(body.get("client"));
        t.endpoint = "preview";
        t.traceId = "preview-" + System.currentTimeMillis();
        t.hasTools = body.get("hasTools") == null || boolOr(body.get("hasTools"), true);
        t.critical = boolOr(body.get("critical"), false);
        Integer estIn = intOrNull(body.get("inputTokens"));
        String category = Json.str(body.get("category"));
        if (category != null && category.isBlank()) category = null;
        policyService.rank(candidates, strategy, t.critical, category, estIn, t.hasTools, t.client, t);
        Map<String, Object> out = t.toJson();
        out.put("candidateCount", candidates.size());
        out.put("strategy", strategy);
        return ResponseEntity.ok(out);
    }

    /** 最近决策列表（内存环形，界面秒开）+ 采样库概况 */
    @GetMapping("/rank-traces")
    public Map<String, Object> rankTraces(@RequestParam(value = "limit", defaultValue = "50") int limit,
                                          @RequestParam(value = "client", required = false) String client,
                                          @RequestParam(value = "model", required = false) String model) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("traces", rankTraceStore.recent(limit, client, model));
        out.put("stored", rankTraceStore.storedInfo());
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        out.put("enabled", d.isRankTraceEnabled());
        out.put("sampleRate", d.getRankTraceSampleRate());
        out.put("keepDays", d.getRankTraceKeepDays());
        return out;
    }

    /** 某一次决策的完整快照（先查内存，落空查采样库；库里没有=那次没被抽中） */
    @GetMapping("/rank-trace/{traceId}")
    public Map<String, Object> rankTrace(@PathVariable("traceId") String traceId) {
        PolicyService.RankTrace t = rankTraceStore.byTraceId(traceId);
        if (t == null) {
            return Map.of("traceId", traceId == null ? "" : traceId, "found", false,
                    "note", "这次决策没有留痕（采样未抽中，或早于该功能上线）。想看排序请用评分沙盘，或提高采样比例。");
        }
        Map<String, Object> out = t.toJson();
        out.put("found", true);
        return out;
    }

    /**
     * 分摊对照：窗口内"实际被调用了几次"对比"按评分该分到几成"。
     * 直接回答用户的疑问——流量是不是怼着几家固定模型，配置里的其它家有没有真分到量。
     *
     * <p>两侧口径都必须在响应里说清，否则界面会把"三成实际量"当成全部：
     * 实际一侧跨全部来源工具（含网关内部压缩调用），{@code client} 非空时只看某一来源；
     * 应得一侧来自采样库+内存环形合并的抽签席权重均值，样本数随响应给出（无观测时界面显示"暂无观测"）。
     */
    @GetMapping("/load-balance")
    public Map<String, Object> loadBalance(@RequestParam(value = "days", defaultValue = "3") int days,
                                           @RequestParam(value = "client", required = false) String client) {
        days = Math.max(1, Math.min(30, days));
        java.time.LocalDate to = java.time.LocalDate.now();
        java.time.LocalDate from = to.minusDays(days - 1L);
        String f = from.toString(), t = to.toString();
        String cl = client == null || client.isBlank() ? null : client;
        List<Map<String, Object>> actual = rankTraceStore.actualUsage(f, t, cl);
        com.gw.policy.RankTraceStore.ShareObservation obs = rankTraceStore.observeShare(
                from.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli(),
                to.plusDays(1).atStartOfDay(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() - 1,
                cl, null);
        Map<String, Double> expected = obs.share();
        long total = 0;
        for (Map<String, Object> r : actual) total += Json.longVal(r.get("requests"));

        ConfigService.Snapshot snap = configService.snapshot();
        List<Map<String, Object>> rows = new ArrayList<>();
        Set<String> called = new HashSet<>();
        for (Map<String, Object> r : actual) {
            String key = r.get("provider") + "\n" + r.get("model");
            called.add(key);
            long req = Json.longVal(r.get("requests"));
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("provider", r.get("provider"));
            m.put("model", r.get("model"));
            m.put("requests", req);
            m.put("tokens", r.get("tokens"));
            m.put("actualPct", total == 0 ? 0 : round1(100.0 * req / total));
            Double share = expected.get(key);
            m.put("expectedShare", share == null ? null : round1(share * 100));
            m.put("expectedRequests", share == null ? null : Math.round(share * total));
            rows.add(m);
        }
        // 配了却一次没被调用的模型：这才是"没分摊下去"的硬证据，逐条列出不靠人肉比对
        List<Map<String, Object>> neverCalled = new ArrayList<>();
        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            if (!p.isEnabled()) continue;
            if (snap.enabledChannels(p.getId()).isEmpty()) {
                neverCalled.add(Map.of("provider", p.getId(), "model", "*", "code", "NO_CHANNEL",
                        "why", "该供应商没有任何启用的渠道"));
                continue;
            }
            for (String model : p.getStaticModels()) {
                if (snap.modelDisabled(p.getId(), model)) {
                    neverCalled.add(Map.of("provider", p.getId(), "model", model, "code", "MODEL_DISABLED",
                            "why", "该模型在路由条目里被停用"));
                    continue;
                }
                if (called.contains(p.getId() + "\n" + model)) continue;
                neverCalled.add(Map.of("provider", p.getId(), "model", model, "code", "ZERO_CALL",
                        "why", "已启用且近 " + days + " 天零调用（分数排不进抽签席、被慢/限流门槛撤席，或从未被抽中）"));
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("days", days);
        out.put("from", f);
        out.put("to", t);
        out.put("client", cl);
        // 取数口径恒显：不说清口径的对照表会被读成反的（曾经"实际"一侧只统计了未标注来源的流量）
        out.put("usageScope", cl == null
                ? "实际侧＝全部来源工具（含网关内部压缩调用 gw-compress）"
                : "实际侧＝仅来源工具 " + cl);
        out.put("usageClients", rankTraceStore.usageClients(f, t));
        out.put("totalRequests", total);
        out.put("rows", rows);
        out.put("neverCalled", neverCalled);
        // 供应商级的"应得"：必须由服务端按决策汇总，不能让界面把模型级均值相加——
        // 模型级均值各自只看"自己进席的那几次"，同家三个模型加总能加出 169% 这种不可能的数。
        Map<String, Object> provExpected = new LinkedHashMap<>();
        obs.providerShare().forEach((p, v) -> provExpected.put(p, round1(v * 100)));
        out.put("providerExpected", provExpected);
        // 本窗口"实际生效"的单家线：封顶取 max(配置值, 1÷该决策占席家数)，占席家数少时比配置值宽松。
        // 界面判"谁超线"要用它，否则两家分席时会把合法的 50% 标成超过 35% 的红字。null=窗口内没有带封顶的决策。
        out.put("providerCapPctEffective", obs.providerCapPctEffective());
        out.put("shareScope", "应得侧＝抽签席 √(偏好分) 权重（偏好分=综合分÷两个速度因子，速度只挡席位不分份额），"
                + "再按供应商封顶回灌；均值分母＝本窗口" + obs.seatedDecisions() + " 次\"有席\"的决策"
                + "（模型级与供应商级同一分母，所以各家应得加起来＝100%），来自采样库＋内存环形去重合并");
        // 封顶参数回显：界面要能在"供应商汇总"那一层直接标出超线的家，而不是让人去系统页翻设置
        GatewayConfig.Defaults lsd = snap.config.getDefaults();
        out.put("providerSeatCap", Math.max(0, lsd.getShareProviderSeatCap()));
        out.put("providerCapPct", lsd.getShareProviderCapPct());
        out.put("observedShareModels", expected.size());
        out.put("shareSamples", obs.samples());
        out.put("shareSamplesFromDb", obs.fromDb());
        out.put("shareSamplesFromMem", obs.fromMem());
        out.put("bandOutReasons", obs.bandOut());
        return out;
    }

    /** 按 Key（渠道）统计：一家供应商配多把 Key 时，看清每一把分别扛了多少请求/多少 token、
     *  撞了多少 429、当前是否在冷却。数据源＝request_log.channel_id（本功能上线前的旧行无归属，
     *  单独成桶并明确标注，绝不摊给在用的 Key）。 */
    @GetMapping("/usage-by-key")
    public Map<String, Object> usageByKey(@RequestParam(value = "days", defaultValue = "3") int days,
                                          @RequestParam(value = "from", required = false) String fromParam,
                                          @RequestParam(value = "to", required = false) String toParam,
                                          @RequestParam(value = "providers", required = false) String providers,
                                          @RequestParam(value = "clients", required = false) String clients,
                                          @RequestParam(value = "models", required = false) String models) {
        // 窗口：用量页传 from/to（与页面其余表同一口径）；都没传才按 days 回溯，最长 30 天（日志只留 30 天）
        java.time.LocalDate today = java.time.LocalDate.now();
        java.time.LocalDate to = parseDay(toParam, today);
        java.time.LocalDate from = parseDay(fromParam, null);
        if (from == null) from = to.minusDays(Math.max(1, Math.min(30, days)) - 1L);
        if (from.isAfter(to)) { java.time.LocalDate t = from; from = to; to = t; }
        if (java.time.temporal.ChronoUnit.DAYS.between(from, to) > 30) from = to.minusDays(30);
        long fromMs = from.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        long toMs = to.plusDays(1).atStartOfDay(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() - 1;
        List<String> pids = csv(providers);
        List<String> cls = csv(clients);
        List<String> mds = csv(models);

        ConfigService.Snapshot snap = configService.snapshot();
        // 配置里每把 Key 的档案（id → 渠道），供统计行补齐"权重/预算/是否启用/掩码"
        Map<String, Map<String, GatewayConfig.Channel>> byProvider = new LinkedHashMap<>();
        for (GatewayConfig.Provider p : snap.config.getProviders()) {
            if (!pids.isEmpty() && !pids.contains(p.getId())) continue;
            Map<String, GatewayConfig.Channel> m = new LinkedHashMap<>();
            for (GatewayConfig.Channel ch : p.getChannels()) m.put(ch.getId(), ch);
            byProvider.put(p.getId(), m);
        }

        List<Map<String, Object>> rows = statsDao.keyUsage(fromMs, toMs, pids, cls, mds);
        long now = System.currentTimeMillis();
        long totalReq = 0;
        for (Map<String, Object> r : rows) totalReq += Json.longVal(r.get("requests"));
        for (Map<String, Object> r : rows) {
            String rp = String.valueOf(r.get("provider"));
            String chId = String.valueOf(r.get("channel"));
            GatewayConfig.Channel ch = chId.isEmpty() ? null
                    : byProvider.getOrDefault(rp, Map.of()).get(chId);
            r.put("keyMasked", ch == null ? null : mask(ch.getApiKey()));
            r.put("configured", ch != null);
            r.put("enabled", ch == null ? null : ch.isEnabled());
            r.put("weight", ch == null ? null : ch.getWeight());
            r.put("dailyTokenLimit", ch == null ? null : ch.getDailyTokenLimit());
            r.put("todayTokens", ch == null ? 0 : policyService.tokensUsedChannel(rp, chId));
            long coolUntil = ch == null ? 0 : policyService.channel(rp, chId).coolDownUntil;
            r.put("coolDownUntil", coolUntil > now ? coolUntil : 0);
            r.put("actualPct", totalReq == 0 ? 0 : round1(100.0 * Json.longVal(r.get("requests")) / totalReq));
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("from", from.toString());
        out.put("to", to.toString());
        out.put("providers", pids);
        out.put("clients", cls);
        out.put("models", mds);
        out.put("totalRequests", totalReq);
        // 取数口径恒显：窗口边界、来源范围、以及"未归属桶"存在的原因，否则旧数据会被读成某把 Key 的量
        out.put("scope", "窗口＝" + from + " ~ " + to + "（按请求落库时间，含网关内部压缩调用与探测背景流量）"
                + (cls.isEmpty() ? "" : "；来源工具 " + String.join("、", cls))
                + (mds.isEmpty() ? "" : "；模型 " + String.join("、", mds))
                + (pids.isEmpty() ? "" : "；供应商 " + String.join("、", pids)));
        out.put("unattributedHint", "channel_id 为空的行＝Key 归属功能上线之前的历史请求，无法判断用的是哪一把，单列不摊给任何一把");
        out.put("rows", rows);
        return out;
    }

    /** yyyy-MM-dd 解析；空或非法一律回落默认值（端点参数不该因手输脏值 500） */
    private static java.time.LocalDate parseDay(String s, java.time.LocalDate orDefault) {
        if (s == null || s.isBlank()) return orDefault;
        try { return java.time.LocalDate.parse(s.trim()); } catch (Exception e) { return orDefault; }
    }

    /** 留痕开关与采样率：改完落盘、下一请求即生效 */
    @GetMapping("/rank-trace-config")
    public Map<String, Object> getRankTraceConfig() {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled", d.isRankTraceEnabled());
        out.put("sampleRate", d.getRankTraceSampleRate());
        out.put("keepDays", d.getRankTraceKeepDays());
        out.put("stored", rankTraceStore.storedInfo());
        return out;
    }

    @PutMapping("/rank-trace-config")
    public ResponseEntity<?> setRankTraceConfig(@RequestBody Map<String, Object> body) {
        Boolean en = body.containsKey("enabled") ? boolOr(body.get("enabled"), false) : null;
        Double rate = doubleOrNull(body.get("sampleRate"));
        Integer keep = intOrNull(body.get("keepDays"));
        if (rate != null && (rate < 0 || rate > 1)) return bad("sampleRate 需在 0~1");
        if (keep != null && (keep < 1 || keep > 90)) return bad("keepDays 需在 1~90");
        if (en == null && rate == null && keep == null) return bad("enabled/sampleRate/keepDays 至少给一个");
        try {
            configService.persist(cfg -> {
                GatewayConfig.Defaults d = cfg.getDefaults();
                if (en != null) d.setRankTraceEnabled(en);
                if (rate != null) d.setRankTraceSampleRate(rate);
                if (keep != null) d.setRankTraceKeepDays(keep);
            });
            return ResponseEntity.ok(Map.of("ok", true));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    private static double round1(double v) { return Math.round(v * 10.0) / 10.0; }

    @PutMapping("/strategy")
    public ResponseEntity<?> setStrategy(@RequestBody Map<String, Object> body) {
        String s = String.valueOf(body.get("strategy"));
        if (!Set.of("priority", "latency", "weighted", "auto", "cost-first", "free-only", "paid-only").contains(s))
            return bad("非法 strategy: " + s);
        // 持久化到配置文件（重启不丢）
        try {
            configService.persist(cfg -> cfg.setStrategy(s));
            return ResponseEntity.ok(Map.of("ok", true, "strategy", s));
        } catch (Exception e) {
            configService.setRuntimeStrategy(s);  // 落盘失败退化为运行时生效
            return ResponseEntity.ok(Map.of("ok", true, "strategy", s, "warn", "仅运行时生效: " + e.getMessage()));
        }
    }

    @GetMapping("/logs")
    public Map<String, Object> logs(@RequestParam(value = "offset", defaultValue = "0") int offset,
                                    @RequestParam(value = "limit", defaultValue = "50") int limit,
                                    @RequestParam(value = "model", required = false) String model,
                                    @RequestParam(value = "realModel", required = false) String realModel,
                                    @RequestParam(value = "provider", required = false) String provider,
                                    @RequestParam(value = "status", required = false) String status,
                                    @RequestParam(value = "statusIn", required = false) String statusIn,
                                    @RequestParam(value = "client", required = false) String client,
                                    @RequestParam(value = "day", required = false) String day,
                                    @RequestParam(value = "from", required = false) String from,
                                    @RequestParam(value = "to", required = false) String to,
                                    @RequestParam(value = "modelsIn", required = false) String modelsIn,
                                    @RequestParam(value = "realModelsIn", required = false) String realModelsIn,
                                    @RequestParam(value = "providersIn", required = false) String providersIn,
                                    @RequestParam(value="clientsIn", required = false) String clientsIn,
                                    @RequestParam(value = "channel", required = false) String channel,
                                    @RequestParam(value = "channelsIn", required = false) String channelsIn,
                                    @RequestParam(value = "sort", required = false) String sort,
                                    @RequestParam(value = "dir", required = false) String dir) {
        // 日期：from/to 区间（单填一个=单边）；day 精确单日（兼容旧参数，from=to=day）
        String dayFrom = from, dayTo = to;
        if ((dayFrom == null || dayFrom.isBlank()) && (dayTo == null || dayTo.isBlank()) && day != null && !day.isBlank()) {
            dayFrom = dayTo = day;
        }
        if (dayFrom != null && dayTo != null && !dayFrom.isBlank() && !dayTo.isBlank() && dayFrom.compareTo(dayTo) > 0) {
            String t = dayFrom; dayFrom = dayTo; dayTo = t;   // 两边都有才可能反，交换防查空
        }
        // *In：逗号分隔多值（页面多选下拉勾选的值，IN 精确匹配，优先于单值 LIKE）
        // statusIn：逗号分隔多状态（页面多选 chips）；单个 status 仍兼容
        List<String> sts = csv(statusIn);
        StatsDao.LogFilter f = new StatsDao.LogFilter(model, realModel, provider, status,
                sts.isEmpty() ? null : sts, client, day,
                csv(modelsIn), csv(realModelsIn), csv(providersIn), csv(clientsIn),
                channel, csv(channelsIn), dayFrom, dayTo);
        List<StatsDao.LogRow> rows = statsDao.pageLogs(offset, Math.min(500, limit), f, sort, dir);
        List<Map<String, Object>> out = new ArrayList<>();
        for (StatsDao.LogRow r : rows) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("ts", r.ts());
            v.put("time", new java.text.SimpleDateFormat("MM-dd HH:mm:ss").format(new Date(r.ts())));
            v.put("model", r.modelReq());
            v.put("provider", r.providerId());
            v.put("realModel", r.modelReal());
            v.put("ttftMs", r.ttftMs());
            v.put("totalMs", r.totalMs());
            v.put("tokensIn", r.tokensIn());
            v.put("tokensOut", r.tokensOut());
            v.put("status", r.status());
            v.put("attempts", r.attempts());
            v.put("profile", r.profile());
            v.put("client", r.client());
            v.put("error", r.error());
            v.put("errorCn", translateError(r.error()));
            v.put("compress", r.compress());
            v.put("traceId", r.traceId());
            v.put("httpStatus", r.httpStatus());
            v.put("errorClass", r.errorClass());
            v.put("channelId", r.channelId());
            out.add(v);
        }
        // 合计（当前过滤条件下的全量统计，不受 limit/offset 影响）
        Map<String, Object> summary = statsDao.logsSummary(f);
        return Map.of("rows", out, "offset", offset, "limit", limit, "summary", summary);
    }

    /** #31 请求链路：按 traceId 反查该客户端请求的全部 request_log 行（多 attempt / 压缩子请求 /
     *  REJECTED 等），按时间升序还原成一条时间线。每行附相对首行的毫秒偏移 deltaMs 与终态标记。 */
    @GetMapping("/trace/{traceId}")
    public Map<String, Object> trace(@PathVariable("traceId") String traceId) {
        List<StatsDao.LogRow> rows = statsDao.logsByTrace(traceId);
        long base = rows.isEmpty() ? 0 : rows.get(0).ts();
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            StatsDao.LogRow r = rows.get(i);
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("seq", i + 1);
            v.put("ts", r.ts());
            v.put("deltaMs", r.ts() - base);
            v.put("time", new java.text.SimpleDateFormat("HH:mm:ss").format(new Date(r.ts())));
            v.put("model", r.modelReq());
            v.put("provider", r.providerId());
            v.put("realModel", r.modelReal());
            v.put("channel", r.providerId() == null ? null : r.providerId() + "/" + r.modelReal());
            v.put("keyId", r.channelId());
            v.put("status", r.status());
            v.put("httpStatus", r.httpStatus());
            v.put("errorClass", r.errorClass());
            v.put("attempts", r.attempts());
            v.put("profile", r.profile());
            v.put("client", r.client());
            v.put("compress", r.compress());
            v.put("ttftMs", r.ttftMs());
            v.put("totalMs", r.totalMs());
            v.put("tokensIn", r.tokensIn());
            v.put("tokensOut", r.tokensOut());
            v.put("error", r.error());
            v.put("errorCn", translateError(r.error()));
            // 终态标记：网关记账顺序决定"最后写入的一行"就是客户端最终拿到的结果行
            v.put("isFinal", i == rows.size() - 1);
            out.add(v);
        }
        boolean legacy = out.isEmpty();   // 迁移前旧行无 trace（链路查询命不中，前端提示"无链路"）
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("traceId", traceId == null ? "" : traceId);
        resp.put("count", out.size());
        resp.put("legacy", legacy);
        resp.put("rows", out);
        // 本次请求的路由决策快照（若被采样抽中）：解释"为什么第一发打给了这家这个模型"
        PolicyService.RankTrace rt = rankTraceStore.byTraceId(traceId);
        resp.put("rank", rt == null ? null : rt.toJson());
        resp.put("rankFound", rt != null);
        return resp;
    }

    /** /api 调用计时窗口（排查管理台卡顿用）：返回服务端自己测到的最近若干次 /api 请求耗时。
     *  前端的 net 尖峰只有和这里对账才能分清"网关算得慢"还是"浏览器到本机这段路慢"。
     *  支持增量：since=上次读到的最大 ts 时只返回该时刻之后的记录，前端不必每轮全量拉取。 */
    @GetMapping("/perflog")
    public Map<String, Object> perflog(
            @RequestParam(value = "since", defaultValue = "0") long since,
            @RequestParam(value = "limit", defaultValue = "60") int limit,
            @RequestParam(value = "minMs", defaultValue = "0") long minMs) {
        List<Map<String, Object>> rows = new ArrayList<>();
        long maxTs = 0;
        for (com.gw.config.ApiTimingFilter.Call c : apiTimingFilter.recent(since, limit)) {
            if (c.ms() < minMs) continue;
            maxTs = Math.max(maxTs, c.ts());
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("ts", c.ts());
            v.put("time", new java.text.SimpleDateFormat("HH:mm:ss").format(new Date(c.ts())));
            v.put("method", c.method());
            v.put("path", c.path());
            v.put("status", c.status());
            v.put("ms", c.ms());
            rows.add(v);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("serverNow", System.currentTimeMillis());
        out.put("windowStart", apiTimingFilter.windowStart());
        out.putAll(apiTimingFilter.counts());
        out.put("maxTs", maxTs);
        out.put("rows", rows);
        return out;
    }

    /** 上游错误信息中文摘要：委托 #9 语义分类器的展示层（单一事实源，不再两处维护模式表）。
     *  未命中返回 null（前端显示原文）。 */
    private static String translateError(String raw) {
        if (raw == null || raw.isBlank()) return null;
        return com.gw.policy.UpstreamErrorClassifier.displayCn(
                com.gw.policy.UpstreamErrorClassifier.classify(null, raw), raw);
    }

    /** 日志过滤下拉选项（type = model_req | model_real | provider_id | client | status）。
     *  client 维度套展示白名单（config log-clients）：空白名单=全展示；非空=只显示勾选过的工具
     *  （测试 UA 等噪音不再出现在下拉里，日志本身照常记录，可按其它维度照常筛出）。 */
    @GetMapping("/log-filter-options")
    public Map<String, Object> logFilterOptions(@RequestParam(value = "type") String type) {
        // usage_* 维度来自 usage_daily（用量页下拉，与用量表展示值同源）；其余来自 request_log
        List<String> values = type != null && type.startsWith("usage_")
                ? statsDao.usageDimensionValues(type)
                : statsDao.logDimensionValues(type);
        if ("client".equals(type) || "usage_client".equals(type)) {
            List<String> wl = configService.snapshot().config.getLogClients();
            if (wl != null && !wl.isEmpty()) {
                Set<String> allow = new HashSet<>(wl);
                values = values.stream().filter(allow::contains).toList();
            }
        }
        return Map.of("values", values);
    }

    /** "来源工具"下拉维护数据：日志里全部来源 + 当前展示白名单（空=全部展示） */
    @GetMapping("/client-display")
    public Map<String, Object> clientDisplay() {
        return Map.of("all", statsDao.logDimensionValues("client"),
                "display", configService.snapshot().config.getLogClients());
    }

    /** 保存"来源工具"展示白名单（body: clients=[...]；空数组=恢复全部展示） */
    @PostMapping("/client-display")
    public ResponseEntity<?> setClientDisplay(@RequestBody Map<String, Object> b) {
        List<String> list = new ArrayList<>();
        if (b.get("clients") instanceof List<?> raw) {
            for (Object o : raw) if (o != null && !String.valueOf(o).isBlank()) list.add(String.valueOf(o));
        }
        try {
            configService.persist(cfg -> cfg.setLogClients(new ArrayList<>(list)));
            return ResponseEntity.ok(Map.of("ok", true, "count", list.size()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /** 同源并发队列诊断：当前各 client 的并发上限 + 排队数 */
    private com.gw.gateway.ClientQueueService clientQueueService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setClientQueueService(com.gw.gateway.ClientQueueService cqs) {
        this.clientQueueService = cqs;
    }

    @GetMapping("/client-queue")
    public Map<String, Object> clientQueueView() {
        return clientQueueService == null
                ? Map.of("enabled", false, "note", "ClientQueueService 未注入")
                : clientQueueService.view();
    }

    /** 自适应限流学习诊断：各 provider 学到的经验限流上限 + 当前窗口用量（含时段/放松状态） */
    @GetMapping("/rate-limit-learn")
    public Map<String, Object> rateLimitLearnView() {
        return policyService.rateLimitView();
    }

    /** 手动重置某家的自适应限流学习（重新从零学）——用于觉得某家上限被学偏了，或市场规则变了 */
    @PostMapping("/rate-limit-learn/reset")
    public ResponseEntity<?> rateLimitLearnReset(@RequestBody Map<String, Object> b) {
        String pid = str(b.get("providerId"));
        if (pid == null || pid.isBlank()) return bad("providerId 必填");
        boolean removed = policyService.resetRateLimit(pid);
        return ResponseEntity.ok(Map.of("ok", true, "providerId", pid, "reset", removed));
    }

    /** 模型巡检：手动触发一轮 + 查看最近一轮结果（ProbeScheduler） */
    private com.gw.probe.ProbeScheduler probeScheduler;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setProbeScheduler(com.gw.probe.ProbeScheduler probeScheduler) {
        this.probeScheduler = probeScheduler;
    }

    @GetMapping("/probe")
    public Map<String, Object> probeView() {
        return probeScheduler == null ? Map.of("results", List.of(), "lastRoundAt", null) : probeScheduler.view();
    }

    @PostMapping("/probe/run")
    public ResponseEntity<?> probeRun() {
        if (probeScheduler == null) return ResponseEntity.status(503).body(Map.of("error", "探测调度器未启用"));
        return CompletableFuture.supplyAsync(() -> {
            probeScheduler.runRoundNow();
            return (Object) Map.of("ok", true);
        }).handle((v, e) -> e == null ? ResponseEntity.ok(v)
                : ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())))).join();
    }

    /** 模型自动停用/恢复：查看状态 + 手动跑一轮 + 开关 + 撤销（ModelReconciler） */
    private com.gw.probe.ModelReconciler modelReconciler;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setModelReconciler(com.gw.probe.ModelReconciler modelReconciler) {
        this.modelReconciler = modelReconciler;
    }

    @GetMapping("/reconcile")
    public Map<String, Object> reconcileView() {
        return modelReconciler == null ? Map.of("enabled", false, "actions", List.of(), "note", "自动停用器未启用")
                : modelReconciler.view();
    }

    @PostMapping("/reconcile/run")
    public ResponseEntity<?> reconcileRun() {
        if (modelReconciler == null) return ResponseEntity.status(503).body(Map.of("error", "自动停用器未启用"));
        return CompletableFuture.supplyAsync(() -> (Object) modelReconciler.runRoundNow())
                .handle((v, e) -> e == null ? ResponseEntity.ok(v)
                        : ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())))).join();
    }

    @PostMapping("/reconcile/toggle")
    public ResponseEntity<?> reconcileToggle(@RequestBody Map<String, Object> b) {
        try {
            boolean hasEnabled = b.containsKey("enabled");
            boolean enabled = Boolean.TRUE.equals(b.get("enabled"));
            Integer interval = b.get("intervalMinutes") instanceof Number n ? n.intValue() : null;
            Integer minFail = b.get("minFailures") instanceof Number n ? n.intValue() : null;
            Integer budget = b.get("probeBudget") instanceof Number n ? n.intValue() : null;
            configService.persist(cfg -> {
                GatewayConfig.Defaults d = cfg.getDefaults();
                if (hasEnabled) d.setAutoDisableEnabled(enabled);
                if (interval != null) d.setReconcileIntervalMinutes(Math.max(0, interval));
                if (minFail != null) d.setReconcileMinFailures(Math.max(1, minFail));
                if (budget != null) d.setReconcileProbeBudget(Math.max(0, budget));
            });
            return ResponseEntity.ok(Map.of("ok", true,
                    "enabled", configService.snapshot().config.getDefaults().isAutoDisableEnabled()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @PostMapping("/reconcile/undo")
    public ResponseEntity<?> reconcileUndo(@RequestBody Map<String, Object> b) {
        if (modelReconciler == null) return ResponseEntity.status(503).body(Map.of("error", "自动停用器未启用"));
        String pid = String.valueOf(b.get("provider"));
        String model = String.valueOf(b.get("model"));
        try {
            modelReconciler.undoDisable(pid, model);
            return ResponseEntity.ok(Map.of("ok", true));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /** 压缩链路一键体检（SelfCheck）：异步启动一轮 + 轮询进度/结果。体检请求全部 client 隔离，
     *  不污染选举/预算/熔断；网关照常服务。 */
    private com.gw.probe.SelfCheckService selfCheckService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setSelfCheckService(com.gw.probe.SelfCheckService selfCheckService) {
        this.selfCheckService = selfCheckService;
    }

    @PostMapping("/selfcheck/run")
    public ResponseEntity<?> selfCheckRun() {
        if (selfCheckService == null) return ResponseEntity.status(503).body(Map.of("error", "体检服务未启用"));
        boolean started = selfCheckService.start();
        return ResponseEntity.ok(Map.of("ok", true, "started", started,
                "note", started ? "体检已启动（串行执行，约 1~2 分钟），请轮询状态查看进度"
                        : "已有一轮体检正在进行中，请等待其完成"));
    }

    @GetMapping("/selfcheck/status")
    public Map<String, Object> selfCheckStatus() {
        return selfCheckService == null ? Map.of("state", "unavailable", "note", "体检服务未启用")
                : selfCheckService.statusView();
    }

    /** #33 toolWeak 自动学习建议清单（仅建议，绝不自动改配置）：满足"带 tools 样本≥阈值且从未回 tool_calls"的模型。
     *  前端一键确认走现有 toolWeak 写回路径（PUT/PATCH 模型）。 */
    @GetMapping("/tool-weak-suggestions")
    public Map<String, Object> toolWeakSuggestions() {
        return policyService.toolWeakSuggestions();
    }

    /** 逗号分隔多值参数 → 去空列表 */
    private static List<String> csv(String raw) {
        if (raw == null || raw.isBlank()) return List.of();
        return java.util.Arrays.stream(raw.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    /** 配置热重载（手动编辑 yaml 后调用） */
    @PostMapping("/config/reload")
    public ResponseEntity<?> reloadConfig() {
        try {
            configService.reload();
            return ResponseEntity.ok(Map.of("ok", true, "loadedAt", configService.snapshot().loadedAtMs));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ================= 工具 =================

    private ResponseEntity<?> persistOr500(java.util.function.Consumer<GatewayConfig> mutator) {
        try {
            configService.persist(mutator);
            catalogService.refreshAll();  // 供应商变了立即刷新目录
            return ResponseEntity.ok(Map.of("ok", true));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private static GatewayConfig.Provider findProvider(GatewayConfig cfg, String id) {
        for (GatewayConfig.Provider p : cfg.getProviders())
            if (p.getId().equals(id)) return p;
        throw new IllegalArgumentException("供应商不存在: " + id);
    }

    private static GatewayConfig.Channel findChannel(GatewayConfig cfg, String pid, String cid) {
        GatewayConfig.Provider p = findProvider(cfg, pid);
        for (GatewayConfig.Channel c : p.getChannels())
            if (c.getId().equals(cid)) return c;
        throw new IllegalArgumentException("渠道不存在: " + pid + "/" + cid);
    }

    private ResponseEntity<Map<String, Object>> bad(String msg) {
        return ResponseEntity.badRequest().body(Map.of("error", msg));
    }

    private static String str(Object v) { return v == null ? null : String.valueOf(v); }
    private static String orDefault(String v, String d) { return v == null || v.isBlank() ? d : v; }
    private static int intOr(Object v, int d) {
        if (v == null) return d;
        try { return Integer.parseInt(String.valueOf(v)); } catch (NumberFormatException e) { return d; }
    }
    private static int intOr(Object v, Integer d) {
        if (v == null) return d == null ? 0 : d;
        try { return Integer.parseInt(String.valueOf(v)); } catch (NumberFormatException e) { return d == null ? 0 : d; }
    }
    private static Integer intOrNull(Object v) {
        if (v == null) return null;
        try { return Integer.parseInt(String.valueOf(v)); } catch (NumberFormatException e) { return null; }
    }
    private static Long longOrNull(Object v) {
        if (v == null) return null;
        try { return Long.parseLong(String.valueOf(v)); } catch (NumberFormatException e) { return null; }
    }
    private static Double doubleOrNull(Object v) {
        if (v == null) return null;
        try { return Double.parseDouble(String.valueOf(v)); } catch (NumberFormatException e) { return null; }
    }
    private static boolean boolOr(Object v, boolean d) {
        if (v instanceof Boolean b) return b;
        if (v instanceof String s) return Boolean.parseBoolean(s);
        return d;
    }
}
