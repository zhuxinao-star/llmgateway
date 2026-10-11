package com.gw.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * YAML 配置模型（config.yaml 映射）。加载后生成不可变快照供各模块读取。
 */
public class GatewayConfig {

    private Server server = new Server();
    private List<Provider> providers = new ArrayList<>();
    private List<Group> groups = new ArrayList<>();
    private Defaults defaults = new Defaults();
    private String strategy = "auto";
    private List<String> gatewayKeys = new ArrayList<>();
    /** 管理面写操作 token（/api/** 的 POST/PUT/DELETE）。缺省=不启用（放行，防自锁）；
     *  配置后写请求需带 x-admin-token 或 Authorization: Bearer（gateway-key 亦可，同源管理）。 */
    private String adminToken;
    /** 请求日志页"来源工具"下拉的展示白名单（空=全部展示）。日志照常记录所有来源，
     *  此列表只控制筛选下拉里出现哪些工具名——测试/一次性 UA 不进列表污染视图。 */
    private List<String> logClients = new ArrayList<>();

    public static class Server {
        private String bind = "127.0.0.1";
        private int port = 8317;
        private String dbPath = "data/llm-gateway.db";

        public String getBind() { return bind; }
        public void setBind(String bind) { this.bind = bind; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getDbPath() { return dbPath; }
        public void setDbPath(String dbPath) { this.dbPath = dbPath; }
    }

    /** 提供商：一家服务 = base_url + 协议 + 基础优先级 */
    public static class Provider {
        private String id;
        private String name;
        private String baseUrl;
        private String protocol = "openai";   // openai | anthropic
        private String proxy;
        private String thinkingMode = "auto";  // auto=按现有实测名单关思考 | disable=该家一律关思考
        private int priority = 5;              // 基础优先级，数字越小越优先
        private String defaultCostTier = "standard";  // free/low/standard/premium
        private boolean enabled = true;
        private List<Channel> channels = new ArrayList<>();
        private List<String> staticModels = new ArrayList<>();  // 不支持 /v1/models 的静态清单
        private Long dailyTokenLimit;          // 提供商整包日预算（可空）
        private Integer concurrencyLimit;      // 提供商级并发上限（可空）
        private BalanceProbe balance;          // 账户余额探测端点（可空=该供应商不支持/未配置）

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        public String getProtocol() { return protocol; }
        public void setProtocol(String protocol) { this.protocol = protocol; }
        public String getProxy() { return proxy; }
        public void setProxy(String proxy) { this.proxy = proxy; }
        public String getThinkingMode() { return thinkingMode; }
        public void setThinkingMode(String v) { this.thinkingMode = v == null || v.isBlank() ? "auto" : v; }
        public int getPriority() { return priority; }
        public void setPriority(int priority) { this.priority = priority; }
        public String getDefaultCostTier() { return defaultCostTier; }
        public void setDefaultCostTier(String t) { this.defaultCostTier = t; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public List<Channel> getChannels() { return channels; }
        public void setChannels(List<Channel> channels) { this.channels = channels; }
        public List<String> getStaticModels() { return staticModels; }
        public void setStaticModels(List<String> staticModels) { this.staticModels = staticModels; }
        public Long getDailyTokenLimit() { return dailyTokenLimit; }
        public void setDailyTokenLimit(Long v) { this.dailyTokenLimit = v; }
        public Integer getConcurrencyLimit() { return concurrencyLimit; }
        public void setConcurrencyLimit(Integer v) { this.concurrencyLimit = v; }
        public BalanceProbe getBalance() { return balance; }
        public void setBalance(BalanceProbe v) { this.balance = v; }
    }

    /** 账户余额探测：一家供应商的只读余额端点（数据驱动，不支持的供应商不配此块）。
     *  path 拼在 baseUrl 后（以 http 开头视为完整 URL）。 */
    public static class BalanceProbe {
        private String path;                       // 相对 baseUrl 的余额路径，或完整 URL
        private List<String> fields;               // 取余额的数字点路径，按序取第一个命中的（如 balance_infos.0.total_balance）
        private List<String> subtractFields;       // 需从 fields 结果中扣减的路径（如 openrouter 的 total_usage），可空
        private String currencyField;              // 币种字段路径（可空=无币种，用 unit 标注）
        private String unit;                       // 无币种时的展示单位（如 "credits"）

        public String getPath() { return path; }
        public void setPath(String v) { this.path = v; }
        public List<String> getFields() { return fields; }
        public void setFields(List<String> v) { this.fields = v; }
        public List<String> getSubtractFields() { return subtractFields; }
        public void setSubtractFields(List<String> v) { this.subtractFields = v; }
        public String getCurrencyField() { return currencyField; }
        public void setCurrencyField(String v) { this.currencyField = v; }
        public String getUnit() { return unit; }
        public void setUnit(String v) { this.unit = v; }
    }

    /** 渠道：提供商下的一把具体 Key */
    public static class Channel {
        private String id;
        private String apiKey;
        private boolean enabled = true;
        private int weight = 1;
        private Long dailyTokenLimit;      // Key 级日预算（可空）
        private Integer concurrencyLimit; // Key 级并发上限（可空）

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getWeight() { return weight; }
        public void setWeight(int weight) { this.weight = weight; }
        public Long getDailyTokenLimit() { return dailyTokenLimit; }
        public void setDailyTokenLimit(Long v) { this.dailyTokenLimit = v; }
        public Integer getConcurrencyLimit() { return concurrencyLimit; }
        public void setConcurrencyLimit(Integer v) { this.concurrencyLimit = v; }
    }

    /** 路由覆盖：(provider, model) 粒度覆盖优先级/费用档/日预算 */
    public static class RoutingOverride {
        private String providerId;
        private String model;
        private Integer priority;
        private String costTier;
        private Long dailyTokenLimit;
        private Integer concurrencyLimit;
        private String alias;   // 归一：真实模型名与此不同时按 alias 转发（可空）
        private String category;  // 擅长类别（code/doc/translate/analysis/general…）——语义路由匹配用
        private Integer contextWindow;  // 上下文上限（token）——请求输入超限时跳过该模型，省白烧 attempt
        private Boolean excludeFromAutoAll;  // true = 不进 autoAll 自动组（图像/视频端点专用模型：聊天请求打到它必失败）
        private Double codeScore;    // 代码能力分 0-10（语义路由 code 场景用；null=未打分不参与）
        private Double userWeight;   // 用户自定义权重 0.1-10（乘法进评分；null=1.0 中性）
        private Boolean toolWeak;    // true=工具调用能力弱：带 tools 的请求评分降权（不剔除，只排后）
        private Boolean excludeFromProbe;  // true=不参与定时探测：配额硬顶的免费模型（如 openrouter/free）探测会烧掉真实日额度
        private Boolean enabled;     // 模型级启停（null/true=启用；false=停用——不展开为路由候选，/v1/models 也不展示）
        private Boolean autoDisabled;  // true=由模型自动停用器因"确认模型不存在"而停（区别于人工停用）；自动恢复只动带此标记的

        public String getProviderId() { return providerId; }
        public void setProviderId(String v) { this.providerId = v; }
        public String getModel() { return model; }
        public void setModel(String model) { this.model = model; }
        public Integer getPriority() { return priority; }
        public void setPriority(Integer priority) { this.priority = priority; }
        public String getCostTier() { return costTier; }
        public void setCostTier(String costTier) { this.costTier = costTier; }
        public Long getDailyTokenLimit() { return dailyTokenLimit; }
        public void setDailyTokenLimit(Long v) { this.dailyTokenLimit = v; }
        public Integer getConcurrencyLimit() { return concurrencyLimit; }
        public void setConcurrencyLimit(Integer v) { this.concurrencyLimit = v; }
        public String getAlias() { return alias; }
        public void setAlias(String alias) { this.alias = alias; }
        public String getCategory() { return category; }
        public void setCategory(String v) { this.category = v; }
        public Integer getContextWindow() { return contextWindow; }
        public void setContextWindow(Integer v) { this.contextWindow = v; }
        public Boolean getExcludeFromAutoAll() { return excludeFromAutoAll; }
        public void setExcludeFromAutoAll(Boolean v) { this.excludeFromAutoAll = v; }
        public Double getCodeScore() { return codeScore; }
        public void setCodeScore(Double v) { this.codeScore = v; }
        public Double getUserWeight() { return userWeight; }
        public void setUserWeight(Double v) { this.userWeight = v; }
        public Boolean getToolWeak() { return toolWeak; }
        public void setToolWeak(Boolean v) { this.toolWeak = v; }
        public Boolean getExcludeFromProbe() { return excludeFromProbe; }
        public void setExcludeFromProbe(Boolean v) { this.excludeFromProbe = v; }
        public Boolean getEnabled() { return enabled; }
        public void setEnabled(Boolean v) { this.enabled = v; }
        public Boolean getAutoDisabled() { return autoDisabled; }
        public void setAutoDisabled(Boolean v) { this.autoDisabled = v; }
    }

    /** 虚拟模型分组 */
    public static class Group {
        private String name;
        private boolean autoAll = false;   // 自动组：动态包含所有供应商全部手工配置模型（static-models），成员列表仅作补充
        private List<Member> members = new ArrayList<>();

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public boolean isAutoAll() { return autoAll; }
        public void setAutoAll(boolean v) { this.autoAll = v; }
        /** 成员列表（YAML 空列表读回可能为 null，统一在此兜底） */
        public List<Member> getMembers() { return members == null ? (members = new ArrayList<>()) : members; }
        public void setMembers(List<Member> members) { this.members = members; }

        public static class Member {
            private String providerId;
            private String model;
            private Integer priority;

            public String getProviderId() { return providerId; }
            public void setProviderId(String v) { this.providerId = v; }
            public String getModel() { return model; }
            public void setModel(String model) { this.model = model; }
            public Integer getPriority() { return priority; }
            public void setPriority(Integer priority) { this.priority = priority; }
        }
    }

    /** 默认运行参数（均可在配置覆盖） */
    public static class Defaults {
        private int connectTimeoutMs = 3000;
        // 流式响应头/首字超时放宽：免费上游高负载时响应头可 >5s、思考型模型首字可 >10s，
        // 旧值(5s/10s)成片误杀慢而活的流，正是"整轮不回复"的根因之一。响应头 30s、首字 60s。
        private int responseHeaderTimeoutMs = 30000;
        private int ttftTimeoutMs = 60000;
        private int channelBudgetMs = 30000;
        private int streamHeartbeatTimeoutMs = 30000;
        private int firstTokenHeartbeatTimeoutMs = 120000;  // 已出过 token 后的流内窗口（真回复可间隔 60~100s，比首字前宽）
        private int sseKeepAliveIntervalMs = 5000;  // 下游 SSE 保活 ping 间隔：静默窗口按此补 `: ping`，防工具读空闲超时断开；0=关闭回退旧路径
        private int totalBudgetMs = 240000;  // 整单总预算：须容得下"放宽后的首字(60s) × 多次候选 + 排队"，否则耐心重试没空间
        // 上下文压缩子调用的交互式硬死线（修复"压缩卡 113s 拖断客户端"）：压缩是非流式，单条超时=通道预算，
        // 旧值恒 120s 且整轮候选链无封顶——免费上游(agnes)一挂就吃满 113s 才吐垃圾，同步阻塞用户那一轮 →
        // Claude Code 客户端主动断开("问题没答完就结束")。现：单条最多 compressCallTimeoutMs，整轮最多
        // compressOverallBudgetMs，超时即停 failover 走硬截断降级。默认单条 45s（>健康 sensenova 全量 39s）、
        // 整轮 60s（用户那一轮最多等 60s 即开始回答，远优于死等到被掐断）。慢点名压缩模型可按需调大。
        private int compressCallTimeoutMs = 45000;
        private int compressOverallBudgetMs = 60000;
        private int maxAttempts = 3;
        private int circuitBreakerThreshold = 3;
        private int circuitBreakerBaseCooldownMs = 60000;
        private int circuitBreakerMaxCooldownMs = 1800000;
        private int rateLimitCooldownMs = 30000;
        private int quotaCooldownMs = 600000;  // 429 配额耗尽（insufficient_quota）的 Key 冷却：短期不会恢复，压长
        private int catalogRefreshMinutes = 5;
        private int catalogFetchTimeoutMs = 10000;
        private double ewmaAlpha = 0.3;
        private boolean catalogDiscovery = false;  // 自动发现上游 /v1/models（默认关：只信手工配置的 static-models）
        private int hedgeDelayMs = 2500;  // 对冲延迟：主选超过该时长未出首字，并发次选竞速（0=关闭）
        private int hedgeMaxContextTokens = 10000;  // 输入超过此 token 数禁用对冲（双发=双倍 TPM，大请求纯串行 failover）
        private int contextCompressThresholdTokens = 0;  // 上下文自动压缩阈值（token，0=关闭）：超过时网关先把历史压成摘要再转发
        private String contextCompressModel;  // 压缩模型（组名或 provider/model 点名）；null=压缩功能关闭
        private int keepRecentMessages = 15;  // 压缩时保留最近 N 条消息原文（其余压成摘要）
        private int keepRecentTokens = 16000; // 压缩保留段 token 预算（0=只看条数）：从最近往前累加，超预算就不再往前扩——
                                              // 少数超大消息（工具输出动辄几万 token）场景下按条数保留压不到阈值下（实测 91K→71K 仍超）
        private int probeIntervalMinutes = 30;      // 定时探测间隔（分钟，0=关闭）：空闲模型定期发 1 token 探测
        private int probeMaxTokens = 8;             // 每次探测的 max_tokens（probe 只测连通/首字速度，产 token 越少越省）
        private int clientMaxConcurrency = 2;       // 同一接入工具(client)的最大并发转发数：超并发排队等上一个完成（0=关闭）
        private int clientMaxConcurrencyWaitMs = 30000;  // 排队等待许可的超时：超时放行撞墙防阻塞（ms）
        private boolean loadShare = true;           // 分摊抽签（auto/priority 策略、非 critical）：排序头部模型按 √(score/max) 概率抽签选主选，
                                                    // 让高分/高优先的健康模型真正分到流量（赢家不再通吃，也避免单家被连发打爆）
        private int shareBandSize = 3;              // 抽签候选池大小（按模型去重后取前 N 个健康高分者）
        private int shareSlowTtftMs = 6000;         // EWMA 首字超过此值的模型不进抽签（"特别慢"的不尝试，只保留兜底顺序）
        private int toolWeakSampleThreshold = 20;   // #33 toolWeak 自动学习：带 tools 请求累计达此样本且从未回 tool_calls → 管理页出建议（仅建议，不自动打标）
        private boolean autoDisableEnabled = false;  // 模型自动停用/恢复总开关（默认关，需显式开）：确认"模型不存在"→永久停用；"余额不足"→临时停用（内存态）
        private int reconcileIntervalMinutes = 60;   // 自动停用器扫描间隔（分钟，0=关闭该调度）
        private int reconcileMinFailures = 2;        // 判定候选所需：近窗口内该模型"模型不存在/余额"失败次数达此值才纳入确认（防一次抖动误判）
        private int reconcileProbeBudget = 5;        // 每轮确认探测预算上限（封顶，防烧免费额度）
        // 路由决策留痕（"评分与分摊"界面的数据源）：默认开 + 5% 采样。
        // 全量落库不可行——一次快照含全部候选的因子分解（5~15KB），agent 连发能到每秒数发，
        // 而转发链路的读写共用一条 SQLite 连接与一把锁，写队列被快照压满就直接拖慢用户那一轮。
        private boolean rankTraceEnabled = true;
        private double rankTraceSampleRate = 0.05;   // 真实请求抽中落库的比例（0~1；内存环形恒收最近 200 次不受此限）
        private int rankTraceKeepDays = 7;           // 采样库保留天数

        public boolean isRankTraceEnabled() { return rankTraceEnabled; }
        public void setRankTraceEnabled(boolean v) { this.rankTraceEnabled = v; }
        public double getRankTraceSampleRate() { return rankTraceSampleRate; }
        public void setRankTraceSampleRate(double v) { this.rankTraceSampleRate = v; }
        public int getRankTraceKeepDays() { return rankTraceKeepDays; }
        public void setRankTraceKeepDays(int v) { this.rankTraceKeepDays = v; }

        public boolean isAutoDisableEnabled() { return autoDisableEnabled; }
        public void setAutoDisableEnabled(boolean v) { this.autoDisableEnabled = v; }
        public int getReconcileIntervalMinutes() { return reconcileIntervalMinutes; }
        public void setReconcileIntervalMinutes(int v) { this.reconcileIntervalMinutes = v; }
        public int getReconcileMinFailures() { return reconcileMinFailures; }
        public void setReconcileMinFailures(int v) { this.reconcileMinFailures = v; }
        public int getReconcileProbeBudget() { return reconcileProbeBudget; }
        public void setReconcileProbeBudget(int v) { this.reconcileProbeBudget = v; }

        public int getHedgeDelayMs() { return hedgeDelayMs; }
        public void setHedgeDelayMs(int v) { this.hedgeDelayMs = v; }

        public int getHedgeMaxContextTokens() { return hedgeMaxContextTokens; }
        public void setHedgeMaxContextTokens(int v) { this.hedgeMaxContextTokens = v; }

        public int getContextCompressThresholdTokens() { return contextCompressThresholdTokens; }
        public void setContextCompressThresholdTokens(int v) { this.contextCompressThresholdTokens = v; }

        public String getContextCompressModel() { return contextCompressModel; }
        public void setContextCompressModel(String v) { this.contextCompressModel = v; }

        public int getKeepRecentMessages() { return keepRecentMessages; }
        public void setKeepRecentMessages(int v) { this.keepRecentMessages = v; }

        public int getKeepRecentTokens() { return keepRecentTokens; }
        public void setKeepRecentTokens(int v) { this.keepRecentTokens = v; }

        public boolean isCatalogDiscovery() { return catalogDiscovery; }
        public void setCatalogDiscovery(boolean v) { this.catalogDiscovery = v; }

        public int getConnectTimeoutMs() { return connectTimeoutMs; }
        public void setConnectTimeoutMs(int v) { this.connectTimeoutMs = v; }
        public int getResponseHeaderTimeoutMs() { return responseHeaderTimeoutMs; }
        public void setResponseHeaderTimeoutMs(int v) { this.responseHeaderTimeoutMs = v; }
        public int getTtftTimeoutMs() { return ttftTimeoutMs; }
        public void setTtftTimeoutMs(int v) { this.ttftTimeoutMs = v; }
        public int getChannelBudgetMs() { return channelBudgetMs; }
        public void setChannelBudgetMs(int v) { this.channelBudgetMs = v; }
        public int getStreamHeartbeatTimeoutMs() { return streamHeartbeatTimeoutMs; }
        public void setStreamHeartbeatTimeoutMs(int v) { this.streamHeartbeatTimeoutMs = v; }
        public int getFirstTokenHeartbeatTimeoutMs() { return firstTokenHeartbeatTimeoutMs; }
        public void setFirstTokenHeartbeatTimeoutMs(int v) { this.firstTokenHeartbeatTimeoutMs = v; }
        public int getSseKeepAliveIntervalMs() { return sseKeepAliveIntervalMs; }
        public void setSseKeepAliveIntervalMs(int v) { this.sseKeepAliveIntervalMs = v; }
        public int getTotalBudgetMs() { return totalBudgetMs; }
        public void setTotalBudgetMs(int v) { this.totalBudgetMs = v; }
        public int getCompressCallTimeoutMs() { return compressCallTimeoutMs; }
        public void setCompressCallTimeoutMs(int v) { this.compressCallTimeoutMs = v; }
        public int getCompressOverallBudgetMs() { return compressOverallBudgetMs; }
        public void setCompressOverallBudgetMs(int v) { this.compressOverallBudgetMs = v; }
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int v) { this.maxAttempts = v; }
        public int getCircuitBreakerThreshold() { return circuitBreakerThreshold; }
        public void setCircuitBreakerThreshold(int v) { this.circuitBreakerThreshold = v; }
        public int getCircuitBreakerBaseCooldownMs() { return circuitBreakerBaseCooldownMs; }
        public void setCircuitBreakerBaseCooldownMs(int v) { this.circuitBreakerBaseCooldownMs = v; }
        public int getCircuitBreakerMaxCooldownMs() { return circuitBreakerMaxCooldownMs; }
        public void setCircuitBreakerMaxCooldownMs(int v) { this.circuitBreakerMaxCooldownMs = v; }
        public int getRateLimitCooldownMs() { return rateLimitCooldownMs; }
        public void setRateLimitCooldownMs(int v) { this.rateLimitCooldownMs = v; }
        public int getQuotaCooldownMs() { return quotaCooldownMs; }
        public void setQuotaCooldownMs(int v) { this.quotaCooldownMs = v; }
        public int getCatalogRefreshMinutes() { return catalogRefreshMinutes; }
        public void setCatalogRefreshMinutes(int v) { this.catalogRefreshMinutes = v; }
        public int getCatalogFetchTimeoutMs() { return catalogFetchTimeoutMs; }
        public void setCatalogFetchTimeoutMs(int v) { this.catalogFetchTimeoutMs = v; }
        public double getEwmaAlpha() { return ewmaAlpha; }
        public void setEwmaAlpha(double v) { this.ewmaAlpha = v; }

        public int getProbeIntervalMinutes() { return probeIntervalMinutes; }
        public void setProbeIntervalMinutes(int v) { this.probeIntervalMinutes = v; }

        public int getProbeMaxTokens() { return probeMaxTokens; }
        public void setProbeMaxTokens(int v) { this.probeMaxTokens = v; }

        public int getClientMaxConcurrency() { return clientMaxConcurrency; }
        public void setClientMaxConcurrency(int v) { this.clientMaxConcurrency = v; }

        public int getClientMaxConcurrencyWaitMs() { return clientMaxConcurrencyWaitMs; }
        public void setClientMaxConcurrencyWaitMs(int v) { this.clientMaxConcurrencyWaitMs = v; }

        public boolean isLoadShare() { return loadShare; }
        public void setLoadShare(boolean v) { this.loadShare = v; }

        public int getShareBandSize() { return shareBandSize; }
        public void setShareBandSize(int v) { this.shareBandSize = v; }

        public int getShareSlowTtftMs() { return shareSlowTtftMs; }
        public void setShareSlowTtftMs(int v) { this.shareSlowTtftMs = v; }
        public int getToolWeakSampleThreshold() { return toolWeakSampleThreshold; }
        public void setToolWeakSampleThreshold(int v) { this.toolWeakSampleThreshold = v; }
    }

    public Server getServer() { return server; }
    public void setServer(Server server) { this.server = server; }
    public List<Provider> getProviders() { return providers; }
    public void setProviders(List<Provider> providers) { this.providers = providers; }
    public List<Group> getGroups() { return groups; }
    public void setGroups(List<Group> groups) { this.groups = groups; }
    public Defaults getDefaults() { return defaults; }
    public void setDefaults(Defaults defaults) { this.defaults = defaults; }
    public String getStrategy() { return strategy; }
    public void setStrategy(String strategy) { this.strategy = strategy; }
    public List<String> getGatewayKeys() { return gatewayKeys; }
    public void setGatewayKeys(List<String> keys) { this.gatewayKeys = keys; }
    public String getAdminToken() { return adminToken; }
    public void setAdminToken(String token) { this.adminToken = token; }
    public List<String> getLogClients() { return logClients == null ? (logClients = new ArrayList<>()) : logClients; }
    public void setLogClients(List<String> v) { this.logClients = v; }

    /** 临时：RoutingOverride 存在于 Provider 内（routing-overrides 列表）*/
    private List<RoutingOverride> routingOverrides = new ArrayList<>();
    // 手写 YAML 里 `routing-overrides:` 空节会被解析成 null → 惰性回退空表防 NPE
    public List<RoutingOverride> getRoutingOverrides() { return routingOverrides == null ? (routingOverrides = new ArrayList<>()) : routingOverrides; }
    public void setRoutingOverrides(List<RoutingOverride> v) { this.routingOverrides = v; }
}
