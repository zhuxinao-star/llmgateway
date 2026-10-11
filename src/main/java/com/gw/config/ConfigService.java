package com.gw.config;

import com.gw.store.Database;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

/**
 * 配置中心：YAML 加载 → 校验 → 不可变快照（含派生的运行时视图）。
 * 请求路径只读快照，热更新时原子换引用。
 */
@Service
public class ConfigService {

    private final Object lock = new Object();
    private volatile Snapshot snapshot;
    /** 运行时覆盖：strategy 切换不写回文件（YAML round-trip 保真复杂），重启后回落配置值 */
    private volatile String runtimeStrategyOverride;

    @Value("${gateway.config-path:config/config.yaml}")
    private String configPath;

    private final Database database;
    private String configFilePath;

    public ConfigService(Database database) {
        this.database = database;
    }

    /** 初始化：加载配置（数据库就绪后调用） */
    public void init() throws IOException {
        this.configFilePath = Path.of(configPath).toAbsolutePath().normalize().toString();
        reload();
    }

    public void reload() throws IOException {
        Path path = Path.of(configFilePath);
        if (!Files.exists(path)) {
            throw new NoSuchFileException("配置文件不存在: " + path + "，请复制 config/config.example.yaml 为 config.yaml 并填入 Key");
        }
        Yaml yaml = newYaml();
        GatewayConfig raw;
        try {
            raw = yaml.loadAs(Files.newBufferedReader(path, StandardCharsets.UTF_8), GatewayConfig.class);
        } catch (YAMLException e) {
            throw new IOException("YAML 解析失败: " + e.getMessage(), e);
        }
        if (raw == null) raw = new GatewayConfig();
        if (runtimeStrategyOverride != null) {
            raw.setStrategy(runtimeStrategyOverride);
        }
        validate(raw);
        synchronized (lock) {
            this.snapshot = buildSnapshot(raw);
        }
    }

    /** 运行时切换策略（不落盘，重启回落配置文件值） */
    public void setRuntimeStrategy(String strategy) {
        this.runtimeStrategyOverride = strategy;
        GatewayConfig cfg = snapshot.config;
        cfg.setStrategy(strategy);  // 快照内配置对象仅此字段热改，读路径无锁
    }

    /**
     * 管理端持久化变更：读取当前文件为基准 → mutator 修改 → 先 validate 校验 → 写回 YAML → 重载换快照。
     * 校验在写盘之前：坏配置不会落地；任何一步失败抛异常（原快照不受影响，服务不中断）。
     */
    public synchronized void persist(java.util.function.Consumer<GatewayConfig> mutator) throws Exception {
        Path path = Path.of(configFilePath);
        // 1) 以当前快照为基准深拷贝（先写回再从文件重载，保证 round-trip 一致）
        Yaml yaml = newYaml();
        GatewayConfig working;
        try {
            working = yaml.loadAs(Files.newBufferedReader(path, StandardCharsets.UTF_8), GatewayConfig.class);
        } catch (Exception e) {
            // 文件读不了（比如首次运行前的场景）就用内存快照
            working = snapshot().config;
        }
        if (runtimeStrategyOverride != null) working.setStrategy(runtimeStrategyOverride);
        mutator.accept(working);
        validate(working);
        // 2) 写回文件（原子替换）
        ConfigWriter.write(working, path);
        // 3) 从文件重载并换快照（保证内存态与磁盘一致）
        reload();
    }

    /** 配置文件路径（管理页展示用） */
    public String getConfigFilePath() { return configFilePath; }

    public Snapshot snapshot() {
        Snapshot s = snapshot;
        if (s == null) throw new IllegalStateException("配置尚未初始化");
        return s;
    }

    private void validate(GatewayConfig c) throws IOException {
        Set<String> providerIds = new HashSet<>();
        for (GatewayConfig.Provider p : c.getProviders()) {
            if (p.getId() == null || p.getId().isBlank()) throw err("provider.id 不能为空");
            if (!providerIds.add(p.getId())) throw err("provider.id 重复: " + p.getId());
            if (p.getBaseUrl() == null || p.getBaseUrl().isBlank()) throw err("provider[" + p.getId() + "].base-url 不能为空");
            if (p.getChannels().isEmpty() && !"ollama".equals(p.getId()))
                throw err("provider[" + p.getId() + "] 至少需要一个 channel");
            Set<String> chIds = new HashSet<>();
            for (GatewayConfig.Channel ch : p.getChannels()) {
                if (ch.getId() == null || ch.getId().isBlank())
                    throw err("provider[" + p.getId() + "] channel.id 不能为空");
                if (!chIds.add(ch.getId()))
                    throw err("provider[" + p.getId() + "] channel.id 重复: " + ch.getId());
            }
        }
        for (GatewayConfig.RoutingOverride ro : c.getRoutingOverrides()) {
            if (ro.getProviderId() == null || !providerIds.contains(ro.getProviderId()))
                throw err("routing-override 引用了不存在的 provider: " + ro.getProviderId());
            if (ro.getModel() == null || ro.getModel().isBlank())
                throw err("routing-override.model 不能为空");
        }
        Set<String> groupNames = new HashSet<>();
        for (GatewayConfig.Group g : c.getGroups()) {
            if (g.getName() == null || g.getName().isBlank()) throw err("group.name 不能为空");
            if (!groupNames.add(g.getName())) throw err("group 名重复: " + g.getName());
            for (GatewayConfig.Group.Member m : g.getMembers()) {
                if (!providerIds.contains(m.getProviderId()))
                    throw err("group[" + g.getName() + "] 引用了不存在的 provider: " + m.getProviderId());
            }
        }
        if (!Set.of("priority", "latency", "weighted", "auto", "cost-first", "free-only", "paid-only").contains(c.getStrategy()))
            throw err("strategy 必须为 priority/latency/weighted/auto/cost-first/free-only/paid-only 之一: " + c.getStrategy());
        if (c.getGatewayKeys().isEmpty())
            throw err("gateway-keys 不能为空：网关需要至少一个本地 Key 防止蹭用");
        if (c.getAdminToken() != null && !c.getAdminToken().isBlank() && c.getAdminToken().trim().length() < 8)
            throw err("admin-token 过短（<8 字符），起不到防护作用；要么留空（不启用）要么 ≥8 字符");
        if (c.getServer().getPort() <= 0 || c.getServer().getPort() > 65535)
            throw err("server.port 非法: " + c.getServer().getPort());
    }

    private IOException err(String msg) { return new IOException("配置校验失败: " + msg); }

    /**
     * 支持 kebab-case（gateway-keys/base-url）映射到 camelCase 字段的 Yaml 实例。
     * 缺属性时跳过（保持默认值）。
     */
    private static Yaml newYaml() {
        LoaderOptions options = new LoaderOptions();
        PropertyUtils pu = new PropertyUtils() {
            @Override
            public Property getProperty(Class<? extends Object> type, String name) {
                // kebab-case → camelCase：gateway-keys → gatewayKeys
                StringBuilder sb = new StringBuilder();
                for (String part : name.split("-")) {
                    if (sb.length() == 0) sb.append(part);
                    else sb.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
                }
                return super.getProperty(type, sb.length() == 0 ? name : sb.toString());
            }
        };
        pu.setSkipMissingProperties(true);
        Constructor c = new Constructor(GatewayConfig.class, options);
        c.setPropertyUtils(pu);
        return new Yaml(c);
    }

    /**
     * 不可变快照：含配置本身 + 派生索引（provider byId、gateway key 哈希、路由覆盖索引）。
     */
    public static final class Snapshot {
        public final GatewayConfig config;
        public final Map<String, GatewayConfig.Provider> providersById;
        public final Map<String, GatewayConfig.RoutingOverride> overrideIndex;  // key: providerId + "\n" + model
        public final Map<String, List<GatewayConfig.RoutingOverride>> overridesByProvider;
        public final Map<String, String> gatewayKeyHashes;  // 明文Key的SHA-256 → keyName
        public final String adminTokenHash;                  // 管理面写操作 token 的 SHA-256；null=未启用
        public final long loadedAtMs;

        Snapshot(GatewayConfig config) {
            this.config = config;
            this.providersById = new HashMap<>();
            for (GatewayConfig.Provider p : config.getProviders()) providersById.put(p.getId(), p);
            this.overrideIndex = new HashMap<>();
            this.overridesByProvider = new HashMap<>();
            for (GatewayConfig.RoutingOverride ro : config.getRoutingOverrides()) {
                overrideIndex.put(key(ro.getProviderId(), ro.getModel()), ro);
                overridesByProvider.computeIfAbsent(ro.getProviderId(), k -> new ArrayList<>()).add(ro);
            }
            this.gatewayKeyHashes = new HashMap<>();
            for (String k : config.getGatewayKeys()) {
                gatewayKeyHashes.put(sha256(k), "gw-" + k.substring(Math.max(0, k.length() - 4)));
            }
            String at = config.getAdminToken();
            this.adminTokenHash = (at == null || at.isBlank()) ? null : sha256(at.trim());
            this.loadedAtMs = System.currentTimeMillis();
        }

        public static String key(String providerId, String model) { return providerId + "\n" + model; }

        public GatewayConfig.RoutingOverride overrideOf(String providerId, String model) {
            return overrideIndex.get(key(providerId, model));
        }

        /** 有效优先级：模型覆盖值（未设置则用提供商基础值） */
        public int effectivePriority(String providerId, String model, Integer groupPriority) {
            if (groupPriority != null) return groupPriority;
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            if (ro != null && ro.getPriority() != null) return ro.getPriority();
            GatewayConfig.Provider p = providersById.get(providerId);
            return p == null ? 99 : p.getPriority();
        }

        /** 模型擅长类别（语义路由用）：routing 条目配置的 category；未配置返回 null（不参与语义加分） */
        public String effectiveCategory(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            return ro == null || ro.getCategory() == null || ro.getCategory().isBlank() ? null : ro.getCategory();
        }

        /** 模型上下文上限（token）：routing 条目 contextWindow；未配置返回 null（视为不限） */
        public Integer effectiveContextWindow(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            return ro == null ? null : ro.getContextWindow();
        }

        /** 是否排除出 autoAll 自动组（图像/视频端点专用模型不参与聊天路由） */
        public boolean excludedFromAutoAll(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            return ro != null && Boolean.TRUE.equals(ro.getExcludeFromAutoAll());
        }

        /** 有效代码能力分（0-10）：routing 条目 codeScore；未配置返回 null（不参与 code 加分） */
        public Double effectiveCodeScore(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            return ro == null ? null : ro.getCodeScore();
        }

        /** 有效用户权重（0.1-10）：routing 条目 userWeight；未配置返回 null（评分按 1.0 中性） */
        public Double effectiveUserWeight(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            return ro == null ? null : ro.getUserWeight();
        }

        /** 工具能力弱标记：带 tools 的请求对该模型评分降权（降权不剔除） */
        public boolean effectiveToolWeak(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            return ro != null && Boolean.TRUE.equals(ro.getToolWeak());
        }

        /** 探测豁免：配额硬顶的免费模型不参与定时巡检（探测烧的是上游真实日额度） */
        public boolean effectiveExcludeFromProbe(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            return ro != null && Boolean.TRUE.equals(ro.getExcludeFromProbe());
        }

        /**
         * 模型是否被停用（enabled=false）。两处命中都算：
         *  1) (provider, model) 精确条目停用；
         *  2) 合并行场景——停用条目挂在请求名上，而路由/autoAll 展开用的是 static 真名（alias），
         *     所以 alias 等于该名的条目停用同样拦住真名。
         *  未配置 enabled（null）或 true 都视为启用（老配置零迁移）。
         */
        public boolean modelDisabled(String providerId, String model) {
            if (model == null) return false;
            GatewayConfig.RoutingOverride exact = overrideOf(providerId, model);
            if (exact != null && Boolean.FALSE.equals(exact.getEnabled())) return true;
            for (GatewayConfig.RoutingOverride x : overridesByProvider.getOrDefault(providerId, List.of())) {
                if (model.equals(x.getAlias()) && Boolean.FALSE.equals(x.getEnabled())) return true;
            }
            return false;
        }

        /** 有效费用档：模型覆盖值（未设置则用提供商默认档） */
        public String effectiveCostTier(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            if (ro != null && ro.getCostTier() != null) return ro.getCostTier();
            GatewayConfig.Provider p = providersById.get(providerId);
            return p == null ? "standard" : p.getDefaultCostTier();
        }

        public Long effectiveDailyTokenLimit(String providerId, String model) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            if (ro != null && ro.getDailyTokenLimit() != null) return ro.getDailyTokenLimit();
            return null; // 条目级未配置
        }

        /** 条目并发上限：条目 → Key → Provider 逐级回退 */
        public Integer effectiveConcurrencyLimit(String providerId, String model, String channelId) {
            GatewayConfig.RoutingOverride ro = overrideOf(providerId, model);
            if (ro != null && ro.getConcurrencyLimit() != null) return ro.getConcurrencyLimit();
            GatewayConfig.Provider p = providersById.get(providerId);
            if (p == null) return null;
            for (GatewayConfig.Channel ch : p.getChannels()) {
                if (ch.getId().equals(channelId) && ch.getConcurrencyLimit() != null) return ch.getConcurrencyLimit();
            }
            return p.getConcurrencyLimit();
        }

        public boolean channelEnabled(String providerId, String channelId) {
            GatewayConfig.Provider p = providersById.get(providerId);
            if (p == null || !p.isEnabled()) return false;
            for (GatewayConfig.Channel ch : p.getChannels()) {
                if (ch.getId().equals(channelId)) return ch.isEnabled();
            }
            return false;
        }

        public List<GatewayConfig.Channel> enabledChannels(String providerId) {
            GatewayConfig.Provider p = providersById.get(providerId);
            if (p == null || !p.isEnabled()) return List.of();
            List<GatewayConfig.Channel> out = new ArrayList<>();
            for (GatewayConfig.Channel ch : p.getChannels()) if (ch.isEnabled()) out.add(ch);
            return out;
        }
    }

    private Snapshot buildSnapshot(GatewayConfig raw) {
        return new Snapshot(raw);
    }

    /** 管理端改了 routing-overrides 等派生索引依赖的配置后重建快照（原子换引用） */
    public void rebuildSnapshot() {
        synchronized (lock) {
            this.snapshot = buildSnapshot(snapshot.config);
        }
    }

    public static String sha256(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
