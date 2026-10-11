# LLM Gateway 概要设计

> 日期：2026-09-09 ｜ 状态：待评审 ｜ 上游文档：[开发计划](./2026-09-09-llm-gateway-开发计划.md)

## 1 概述

### 1.1 目的与范围

本地单机运行的**大模型 API 网关**：统一管理多家服务商的 API Key（提供商→渠道→模型三层），对外暴露统一的 OpenAI 兼容端口；按"优先级 × 健康度 × 速度 × 费用档位"综合评分选路，失败自动切换，全链路超时兜底，并对每日 token 额度做预算治理。

**非目标（v1 明确不做）**：多租户与用户体系、计费账单、RBAC 权限、OpenAI Responses API、图像/音频生成接口。

### 1.2 术语

| 术语 | 含义 |
|---|---|
| 提供商 Provider | 一家大模型服务（一个 base_url + 协议类型），可配基础优先级 |
| 渠道 Channel | 提供商下的一把具体 API Key，多 Key 轮转分摊限速 |
| 路由条目 RoutingEntry | 路由的最小决策单元 = (提供商, 模型)，可覆盖优先级、费用档、日预算 |
| 模型目录 Catalog | 自动发现聚合的全量"模型→提供商"清单，内存哈希索引 |
| 虚拟模型 Group | 用户自定义模型名，组内成员是多个 (提供商, 模型) 条目 |
| 渠道预算 channel budget | 单次请求在某候选渠道上的最大耗时（默认 30s） |

## 2 总体架构

```
                    AI Agent (Claude Code / Cline / 自研)
                              │ OpenAI 兼容协议
┌─────────────────────────────▼─────────────────────────────────┐
│  gateway-api 接入模块        SSE 流式透传 · 本地 Key 鉴权       │
├───────────────────────────────────────────────────────────────┤
│  router 路由模块  模型目录匹配 → RoutingEntry 候选 → 排序       │
│  policy 策略模块  评分公式 · EWMA · 熔断 · 429 · 额度预算       │
├───────────────────────────────────────────────────────────────┤
│  forward 转发引擎  超时预算 · 失败切换 · 多 Key 轮询            │
│  provider-adaptor 协议适配 SPI  (openai / anthropic 实现)      │
├───────────────────────────────────────────────────────────────┤
│  stats 计量模块(异步)   config 配置中心   store 持久化(SQLite)  │
│  catalog 模型发现(后台定时)              admin 管理端           │
└───────────────────────────────────────────────────────────────┘
     ▼ 按渠道适配后的原生请求（可选 HTTP 代理）
 OpenAI / Anthropic / DeepSeek / 智谱 / 月之暗面 / 硅基流动 / Ollama …
```

### 2.1 模块清单

| 模块（Java 包） | 职责 | 依赖 |
|---|---|---|
| `gateway` | 暴露 /v1/* 接口；鉴权；请求解析；SSE 写出；错误模型 | router, stats |
| `router` | 模型名→候选 RoutingEntry 列表（哈希索引 O(1)）；前缀点名；虚拟模型展开 | catalog, policy |
| `policy` | 评分排序；EWMA/熔断/冷却/预算水位等运行时状态的权威读写 | store(快照) |
| `forward` | HTTP 客户端执行；三层超时预算；首 token 前失败重试下一候选 | provider-adaptor |
| `provider.adaptor` | 协议转换 SPI：请求/响应/SSE 事件双向转换 | 无（纯函数，易单测） |
| `catalog` | 后台定时拉取各家 /v1/models；聚合目录；变更通知 | provider.adaptor |
| `stats` | TTFT/状态/usage 计量事件队列 → 异步聚合落库；预算计数 | store |
| `config` | YAML 加载校验；不可变配置快照；热更新 | store(密钥密文) |
| `store` | SQLite 访问层；运行时状态快照 | 无 |
| `admin` | /api/* 管理类 REST + /ui statis 单页 | policy/router/config/stats |

**依赖规则**：单向依赖（上→下），policy 是运行时状态唯一权威，stats 只发事件不改状态，避免两处计数打架。

## 3 数据模型

### 3.1 配置实体（config.yaml 映射）

| 实体 | 字段 | 说明 |
|---|---|---|
| Provider | id/name、base-url、protocol、proxy、priority(基础优先级 int)、default-cost-tier(费用默认档) | proxy 为该家单独配 HTTP 代理 |
| Channel | provider-id、api-key(密文)、enabled、weight | 一 Provider 多 Channel |
| RoutingOverride | provider-id、model、priority(覆盖)、cost-tier(free/low/standard/premium)、daily-token-limit、concurrency-limit | 不配则继承 Provider |
| ProviderTotalBudget | provider-id、daily-token-limit | 提供商全部楼层闸 |
| Group(虚拟模型) | name、members[{provider-id, model, priority}] | 引用展开用 |
| Gateway | bind(默认 127.0.0.1)、port(默认 8317)、gateway-keys[]、defaults、strategy | strategy 见 §4.4 |

### 3.2 内存实体

| 实体 | 关键字段 | 说明 |
|---|---|---|
| CatalogEntry | model、provider-id、real-model-name、sources(发现/静态/手工) | 目录行 |
| RoutingIndex | `Map<modelKey, List<RoutingEntry>>` 不可变快照 | modelKey 含前缀与组名 |
| RoutingEntry | provider-id、model、effective-priority、cost-tier、limits | 由配置快照×目录派生 |
| ChannelRuntime | 每把 Key 一份：state(AVAILABLE/COOLING/DISABLED)、cool-down-until、rotation 计数 | Key 级冷却（如 429 只打中这把 Key）不连坐同提供商其它 Key |
| EntryRuntime | 每个 RoutingEntry 一份：ewma-ttft-ms、consecutive-failures、cool-down-until、last-error | 熔断与 EWMA 按 提供商×模型 维度（同家多 Key 共享上游，延迟不必分 Key），重启可恢复 |
| UsageCounter | tokens_today_in/out、requests_today（provider×model 与 provider 两层） | 内存实时累计，SQLite 兜底 |

**实体关系**：Provider 1—N Channel；Provider × CatalogEntry 派生 RoutingEntry（可被 RoutingOverride 覆盖）；Group 引用 RoutingEntry；ChannelRuntime↔Channel 一对一；EntryRuntime↔RoutingEntry 一对一。

## 4 核心流程

### 4.1 请求主流程（v7.7 现行版）

```
POST /v1/chat/completions / POST /v1/messages（Anthropic 原生，自动协议转换）
 1 鉴权：本地 Key 比对；ClientDetector 识别来源工具（贯穿日志/用量）
 2 解析 model：`provider/model` 前缀点名 | 组名（free-fast 等，autoAll 自动组
   动态含全部启用供应商的手工模型；excludeFromAutoAll 条目剔除）| 直接模型名；
   未知模型名 → 回落 free-fast 组（工具侧永不 404）
 3 RouterService 查候选（供应商×模型×每把启用 Key 各一条）
 4 上下文自动压缩（可选，超阈值时）：system 全保留 + 最近 N 条原文 +
   其余历史→摘要（详见 §4.8）
 5 policy.rank()：剔除 DISABLED/冷却中/预算耗尽/并发满/上下文超限（contextWindow
   ×1.1 余量）→ 按 Score 排序；同 (provider,model) 多 Key 按 lastUsedAt 严格轮转
 6 attempt 循环（候选穷尽，上限 6，钳到候选数；总预算内）：
   ├ HedgedExecutor：主选先发；hedgeDelayMs 无首字→并发次选竞速
   │  （陪跑仅免费档；输入>hedgeMaxContextTokens 禁对冲；非流式退化单发）
   ├ forward.execute()：请求转换+按渠道注入关思考参数+消息规范化（tool 序重排）
   ├ 首字后 streamToClient：双档心跳看门狗（首字前严/出字后宽）+[DONE] 主动结束
   └ 失败/超时/429 → recordFailure（熔断/Key 冷却）→ 下一候选
 7 候选穷尽：透传最后上游错误；响应头 X-Gateway-Channel / X-Gateway-Attempt /
   X-Gateway-Context-Compressed 便于排查
```

**非流式**：同链路整包返回。虚拟线程，每 provider 复用连接池。图像生成走独立透传端点 /v1/images/generations（候选/评分/记账复用）。多模态同类透传端点：/v1/embeddings（向量）、/v1/videos/generations（视频，长超时 120s）——模型不存在时明确 404 提示（不盲目回落 free-fast 文本组）。

### 4.2 超时与兜底（响应速度保障，参数均可配）

| 环节 | 默认值 | 超时动作 |
|---|---|---|
| TCP 连接 | 3s | 立即换下一候选（连接根本未建立，属快速失败，不占 attempt 重试次数） |
| 响应头到达 | 5s | 同上 |
| 首 token（TTFT） | 10s | 掐断换下一候选 |
| 渠道总预算 | 30s | 掐断换下一候选 |
| 流中心跳 | 30s | 上游 SSE 两个事件间超此值→掐断（进入流后的死角兜底） |
| 请求总预算 | 120s | 所有候选共享总预算，到即透传最后错误 |

**重试铁律**：仅在「首 token 之前」失败才允许换渠道（已开始输出就不断流不误伤）；上游 5xx 与网关超时计入熔断，429 走冷却快通道（默认 30s 起）不触发熔断计数。

### 4.3 熔断状态机

```
CLOSED ──连续3次失败(或并发超限)──► OPEN ──退避到期──► HALF_OPEN
  ▲   冷却：60s 起步，每跳翻倍，封顶 30min                │
  └──────────────半开单请求成功──────────────────────────┘
                  半开失败 → 回 OPEN（退避×2）
OPEN 期间：不参与选路；若该 (provider,model) 是唯一候选，放行「最后兜底」单请求
```

### 4.4 评分与 EWMA（现行版）

```
Score = P(优先级) × H(健康) × S(首字速度) × T(吞吐) × C(费用) × K(语义类别)
        × CS(代码能力·性价比) × U(用户权重) × R(限流恢复) × L(限流压力) × Q(输出质量)
  P：40/(1+priority×5)，收窄档距让 S/T/K 有实际话语权（latency 策略 P=1）
  S：EWMA TTFT 归一化倒数；无样本=免费 0.9 / 付费 0.5（压档防反超，见下）
  T：EWMA 出token/秒；无样本同 S 哲学
  C：free 1.0 / low 0.8 / premium 0.1；critical 豁免
  K：请求语义类别（关键词规则判 code/doc/analysis）命中模型擅长标签 ×1.8，
     general 万金油 ×1.0，不匹配 ×0.7
  CS：代码能力分 + 性价比（仅 code 类请求且模型配了 codeScore 时生效）——
     (1+codeScore/10) × 成本惩罚(free 1.0/low 0.9/standard 0.75/premium 0.5)，
     等价"能力/成本"：能力高且免费最优先，能力高但积分贵自动打折；未配置=1.0
  U：用户自定义权重（管理页手调，1.0 中性，>1 拔高/<1 压低）——未配置=1.0
  R：限流恢复因子（刚 429 过的渠道宽限期内降权，防冷却到期立刻再打爆）
  L：自适应限流压力（当前窗口用量接近学到的上限时降权，预测性避让）
  Q：输出质量（截断/空回复/JSON不合法等软失败降权，乘最外层）
EWMA: s_t = α·x_t + (1-α)·s_{t-1}，α=0.3；TTFT/吞吐双维度（按 提供商×模型）
```

**三条防污染规则**（实战教训沉淀）：
- 输入>10K token 的请求 TTFT 不进 EWMA（预填充慢≠模型慢；吞吐照记）
- 无样本压档：免费 0.9/付费 0.5——"无历史包袱"的付费模型不能凭默认分反超有真实样本的免费模型（12:13 付费反超案根因）
- 429 区分限流（30s Key 冷却）与配额耗尽 insufficient_quota（10min 长冷却）

策略模式 `strategy`：`priority` / `latency` / `weighted`（top3 加权随机）/ `auto`（默认）/ `cost-first`。`X-Gateway-Profile: critical` 豁免 C 因子与预算。多 Key 轮转：rank 排序按 lastUsedAt 严格交替（刚用过的 Key 排后），一号限速二号自动顶上。

### 4.5 额度预算（防"一下烧光"）

- 计量来源：上游响应的 usage 字段（流式从最后一个含 usage 的事件提取；无 usage 时按 4字符≈1token 估算并标注 estimated）。
- 判定：请求**前**检查 RoutingEntry 当日 tokens ≥ daily-token-limit → 摘除（改走下一优先级）；提供商总预算同理。
- 重置：本地日期分界自动重置（内存计数器以 `day` 字段自比对）；SQLite 保留历史供报表。
- 预警：70%/90% 水位由 admin 页面展示并可发 Windows 通知；手动 `POST /api/budget/reset` 即时恢复。

### 4.6 模型目录发现

启动后异步首轮拉取 → 之后每 5 分钟定时刷；单次拉取超时 10s，失败沿用旧目录并标 stale；各家不支持 /v1/models 的走静态清单（配置的 `static-models`）；目录变更→重建不可变 RoutingIndex →原子换引用。

### 4.7 配置热更新

admin API 写 /config 或文件变更 → config 校验重建不可变快照 → 派生 RoutingIndex/目录一起换 → EntryRuntime/ChannelRuntime 按各自稳定键（提供商×模型 / channel-id）保留运行状态。请求路径读快照，无锁无竞态。

### 4.8 上下文自动压缩（v7.7）

Agent 工具每轮携带全部会话历史（实测 DSH 164K token），三重危害：小窗口模型 400、
TPM 配额双烧、EWMA 被大请求刷慢。压缩链路：

```
请求输入 > contextCompressThresholdTokens（默认关，生产 100K）
  ├ 分段：开头 system 段全保留 | 历史段 | 最近 keepRecentMessages 条原文保留
  │        （边界对齐工具组：assistant(tool_calls)+tool 不可拆，防孤儿 tool 消息）
  ├ 滚动缓存（性能关键）：按客户端存 前缀SHA-256+摘要+截点。
  │        Agent 历史 append-only → 前缀哈希命中则只压增量（旧摘要+新增消息→合并摘要，
  │        实测 6ms）；未命中才全量压（143K→40K 约 40s）
  ├ 压缩模型候选链：contextCompressModel 支持组名/点名。组名→复用 policy.rank 全套
  │        治理 + 费用守卫（非 free 档候选一律跳过——免费政策变更不产生账单）；
  │        点名（含 /）→ 用户显式授权，付费也尊重
  ├ 保护：输入超首位候选窗口×0.8 截最旧；摘要<200字符(>5K输入)判敷衍换下一家；
  │        压缩调用预算 240s（独立于聊天 30s 通道预算）
  └ 重组：system 段 + 摘要（system 身份）+ 保留段 → 替换原 messages 参与路由
铁律：压缩任何环节失败 → 原样转发（功能永不成为请求失败的原因）
```

标记：响应头 `X-Gateway-Context-Compressed: true`；app.log "上下文压缩完成：约 X K → X K token"。

## 5 接口设计

### 5.1 对外（Agent 侧，OpenAI 兼容）

| endpoint | 方法 | 说明 |
|---|---|---|
| /v1/chat/completions | POST | stream / 非 stream |
| /v1/models | GET | 聚合目录：原始模型名 + `provider/model` 全名 + 组名 |
| /v1/embeddings | POST | 阶段 4；仅 OpenAI 兼容提供商 |

鉴权 `Authorization: Bearer <gateway-key>`；可选头 `X-Gateway-Profile: critical`。

### 5.2 管理（/api/*，仅本机可写）

| endpoint | 说明 |
|---|---|
| GET /api/overview | 各渠道 state/TTFT/失败率/今日额度水位 |
| GET /api/usage?date= | tokens/费用 按 provider×model×天 聚合 |
| GET /api/logs | 请求日志分页过滤 |
| PUT /api/routing/{provider}/{model} | 改优先级/费用档/日预算（热生效） |
| POST /api/channels/{id}/enable·disable | 手动启停 |
| POST /api/budget/reset | 即时重置额度 |
| POST /api/catalog/refresh | 立即刷新模型目录 |
| GET·PUT /api/strategy | 策略模式与权重 |
| GET·PUT /api/keys | gateway-key 管理（仅显示后 4 位） |

### 5.3 错误模型

网关自身错误统一返回 OpenAI 错误格式：`{"error":{"message","type":"gateway_error","code"}}`，code ∈ `unauthorized / model_not_found / no_candidate / all_attempts_failed`；候选穷尽时透传上游原始错误体（状态码沿用）。

## 6 持久化设计（SQLite，WAL 模式）

```sql
CREATE TABLE usage_daily (day TEXT, provider_id TEXT, model TEXT,
  req_count INTEGER DEFAULT 0, tokens_in INTEGER DEFAULT 0,
  tokens_out INTEGER DEFAULT 0, cost_real REAL DEFAULT 0,
  PRIMARY KEY(day, provider_id, model));
CREATE TABLE request_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER,
  model_req TEXT, provider_id TEXT, model_real TEXT,
  ttft_ms INTEGER, total_ms INTEGER, tokens_in INTEGER, tokens_out INTEGER,
  cost_real REAL, status TEXT, attempts INTEGER, profile TEXT, error TEXT);
CREATE INDEX idx_log_ts ON request_log(ts);
CREATE TABLE runtime_snapshot (provider_id TEXT, channel_id TEXT DEFAULT '*',
  model TEXT, ewma_ttft_ms REAL, state TEXT, cool_down_until INTEGER,
  consecutive_failures INTEGER, updated_at INTEGER,
  PRIMARY KEY(provider_id, channel_id, model));
```

写入策略：stats 事件进内存队列 → 批量异步写（1s 或 100 条触发）；request_log 保留 30 天定期清理；runtime_snapshot 两级行（条目行 channel_id='*'；Key 冷却行存具体 channel_id）每 60s 落一次、启动时恢复（重启不丢熔断/EWMA/当日额度状态，当日额度以 usage_daily 当日行为准）。API Key 密文存 SQLite `secret` 表（AES-256-GCM，主密钥文件 `%APPDATA%\llm-gateway\master.key`，权限限定当前用户）。

## 7 非功能设计

| 项 | 目标/方案 |
|---|---|
| 性能 | 网关自身开销（除上游）实测 P95 < 50ms@300 模型×50 渠道；选路为不可变快照哈希查找 |
| 并发 | Java 21 虚拟线程承载 SSE 长连接，100+ 并发会话无压力 |
| 安全 | 默认仅绑 127.0.0.1；gateway-key SHA-256 哈希比对；Key 密文存储；日志 Key 脱敏（`sk-abcd…wxyz`） |
| 日志 | logback 分级滚动；请求日志单独 logger |
| 部署 | 单 fat.jar，`java -jar llm-gateway.jar --config=D:\gw\config.yaml`；Windows 下配 nssm/任务计划开机自启 |

**技术栈**：JDK 21 + Spring Boot 3（web + 虚拟线程）+ Java HttpClient + SnakeYAML + sqlite-jdbc + 原生 HTML 单页（/ui）。

## 8 测试要点（概要）

1. **适配器黄金回放**：录制各家真实响应（含流式分片）作黄金样本，转换后逐字段断言——协议漂移是最大风险源；
2. 超时注入测试（本地代理限速）验证 §4.2 全部分支与"流中 30s 掐断"；
3. 熔断/冷却状态机单元测试（时钟注入）；
4. 预算边界：usage 缺失回退估算、流式末事件 usage 接住、跨日重置；
5. 并发压测：100 路流式长连接下内存与延迟曲线。
