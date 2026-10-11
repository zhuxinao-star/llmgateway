# llmGateway — 本地大模型 API 网关

统一管理多家大模型服务商的 Key，对外暴露 OpenAI 兼容接口，按「优先级 × 健康度 × 速度 × 费用」智能选路，失败自动切换，全链路超时兜底，每日 token 额度治理。

技术栈：JDK 21 + Spring Boot 3 + 虚拟线程 + Java HttpClient + SQLite（嵌入式，无需安装）。

---

## 一、快速开始

### 1. 构建与启动

需要 JDK 21（Spring Boot 3 用虚拟线程，17 也跑不起来）。构建产物是单个 fat jar，运行不依赖任何外部数据库。

**Windows：仓库自带脚本，直接跑就行**

```bat
scripts\doctor.bat      :: 先自检：它把找到的 JDK/Maven/端口/库/日志位置逐条打出来
scripts\build.bat       :: 产出 target\llmGateway.jar
copy config\config.example.yaml config\config.yaml   :: 填你自己的上游 Key
scripts\start.bat
```

`stop.bat` / `status.bat` / `restart.bat` 是配套的停服、探活、重启。

**不用脚本（Linux/Mac 或你已有工具链）**：

```bash
mvn -q package -DskipTests          # 产出 target/llmGateway.jar
java -jar target/llmGateway.jar     # 工作目录＝项目根（要能读到 config/config.yaml）
```

首次运行先准备配置：把 `config/config.example.yaml` 复制为 `config/config.yaml`，填入你自己的上游 Key。
`config/config.yaml` 已被 `.gitignore` 排除（里面是真实密钥），提交前请再确认一次。

启动成功标志：`logs/app.log` 出现 `Tomcat started on port 8317`，或 `scripts\status.bat` 打 `UP HTTP=200`。
探活：`GET http://127.0.0.1:8317/api/overview`（没有 `/api/health` 端点）。

**下载后要不要改脚本里的路径？基本不用。** 七个脚本的路径只在一处解析（`scripts\_env.bat`）：项目根＝`scripts\` 的上一级（并校验那里有 `pom.xml`，没有就报 `[layout error]` 退出），JDK 按 `GW_JDK` → `tools\jdk-*` → `JAVA_HOME` → PATH 找，Maven 按 `GW_MVN` → `tools\apache-maven-*\mvn.cmd` → PATH 找，数据和日志默认落项目根下的 `data\`、`logs\`。克隆到任何目录、路径带空格括号都不影响。要挪动或指定，用这七个环境变量（同一个窗口里设好再调用脚本）：

| 变量 | 作用 |
|---|---|
| `GW_JDK` | 指到 JDK 21 目录（含 `bin\java.exe` 的那层，不是 `bin`） |
| `GW_MVN` | 指到 `mvn.cmd` |
| `GW_BASE` | 一次把 `data\` 和 `logs\` 挪到别处 |
| `GW_DB_PATH` · `GW_LOG_DIR` | 单独指定库 / 日志位置（优先于 `GW_BASE`） |
| `GW_PORT` · `GW_HOST` | 换监听端口 / 网卡（脚本注入 `-Dserver.port`、`-Dserver.address`，确实生效） |

改完跑 `scripts\doctor.bat` 确认，不用猜。**端口那条有个坑**：真正决定监听的是 `src/main/resources/application.yaml` 的 `server.port`，`config/config.yaml` 里的 `server: bind/port` 只用来打印启动那一行日志，改它不生效——细节与实测见 `docs/部署指南.md` 第 3 节。

### 2. Agent 工具接入（ZCode / Cline / Claude Code 等）

**OpenAI 兼容客户端（ZCode / Cline / 自研 Agent）**：

| 配置项 | 值 |
|---|---|
| base_url | `http://127.0.0.1:8317/v1` |
| API Key | `<你的网关 Key>`（config.yaml 的 `gateway-keys`） |
| 模型 ID | 见下表 |

**Claude Code（Anthropic 原生协议）**——网关已实现 `/v1/messages` 双向协议转换，Claude Code 可直连：

```bash
# 方式一：环境变量
export ANTHROPIC_BASE_URL=http://127.0.0.1:8317
export ANTHROPIC_AUTH_TOKEN=<你的网关 Key>     # 或 ANTHROPIC_API_KEY
claude

# 方式二：settings.json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://127.0.0.1:8317",
    "ANTHROPIC_AUTH_TOKEN": "<你的网关 Key>"
  }
}
```

Claude Code 发 Anthropic 格式请求 → 网关转成 OpenAI 格式走智能路由（免费快档优先/failover/超时兜底全部生效）→ 响应转回 Anthropic 格式（含流式 SSE 事件、工具调用 tool_use/tool_result、usage）。**注意**：Claude Code 里选的模型名要用网关里的名字（如 `deepseek-v4-flash`、`free-fast` 组名），可在启动参数 `--model deepseek-v4-flash` 指定。

**模型 ID 怎么选**：

| 类型 | 示例 | 行为 |
|---|---|---|
| 虚拟组（推荐） | `free-fast` / `deepseek` / `glm` | 组内多供应商自动选路，免费快档先用 |
| 具体模型名 | `deepseek-v4-flash` / `glm-5.2` | 所有支持该模型的渠道自动候选 |
| 前缀点名 | `amd/deepseek-v4-flash` | 强制走指定供应商 |
| 分组/真实名不一致 | 组内 `Qwen3.8-Flash-Next` | 网关用 `alias` 自动还原真实名转发 |

流式测试命令：

```bash
curl -N http://127.0.0.1:8317/v1/chat/completions \
  -H "Authorization: Bearer <你的网关 Key>" -H "Content-Type: application/json" \
  -d '{"model":"free-fast","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

---

## 二、管理台使用（核心日常操作）

打开 **http://127.0.0.1:8317/ui** ，四个标签页：

### ① 供应商（增删改供应商 / 渠道 / 模型路由）

- **新增供应商**：点「+ 新增供应商」→ 填 ID、baseUrl、协议（openai 兼容直通 / anthropic 自动转换）、优先级、费用档、apiKey → 保存。**立即生效并持久化到 config/config.yaml，重启不丢**。
- **编辑供应商**：卡片右上「编辑」→ 可改名称/baseUrl/协议/优先级/费用档/换 Key/代理。apiKey 留空 = 不修改。
- **删除供应商**：「删」→ 确认。会连同该家的渠道、模型清单、模型路由条目、分组成员引用一起清理。
- **停用/启用**：「停用」按钮临时下线（持久化），流量立即绕开。
- **模型管理（每家供应商的模型增删改）**：供应商卡片内嵌「模型清单」表格——
  - **+ 添加模型**：填模型名，可一并设优先级覆盖/费用档/日 Token 限/别名 alias；
  - **编辑**：改模型名（改名会同步更新路由条目和分组成员引用）、调优先级、换费用档、改日限、改别名；
  - **删**：连同该模型的路由条目和分组成员引用一起清理。
  - 模型清单之外的：目录自动发现（各家 /v1/models 每 5 分钟拉一次）兜底，两者合并生效。
- **渠道（Key）管理**：每张供应商卡片内，「+ 添加渠道」加多把 Key（轮转分摊限速）；「编辑/删」管理已有 Key。最后一把 Key 不允许删。
- **模型路由条目**（页面下方表格）：某供应商 × 某模型的精细化覆盖——优先级、费用档（free/low/standard/premium）、日 Token 上限、别名 alias（上游真实模型名）。新增/编辑/删除均持久化。
- **虚拟模型分组**：定义组名（如 `free-fast`），成员 = 多个 (供应商, 模型, 组内优先级)。Agent 请求组名，网关在组内选路。

### ② 用量统计

- 供应商 × 模型 维度的 **请求次数 / 输入 Token / 输出 Token / 合计**，支持今天/近7天/近30天/近一年。
- 日趋势表：每天的请求数与 Token 总量水位条。
- 数据来源：SQLite `usage_daily` 表按天聚合，重启不丢。

### ③ 请求日志

- 最近 100 条请求：时间、请求模型、实际选中的供应商/真实模型、TTFT、总耗时、tokens、状态、尝试次数、错误信息。
- 可按模型名过滤。底层保存在 SQLite `request_log`（自动清理 30 天前的）。

### ④ 系统

- **选路策略切换**：auto（综合，默认）/ priority（严格优先级）/ latency（速度优先）/ weighted（加权随机）/ cost-first（费用优先）/ free-only（只走免费：候选硬过滤到免费档，全挂直接失败、绝不付费兜底）/ paid-only（只走付费：候选硬过滤到付费档，只看速度/成功率/编码能力不看成本与优先级）。持久化。
- **运行时状态**：每 (供应商×模型) 的熔断状态、EWMA TTFT、连续失败次数、最近错误。
- **维护操作**：刷新模型目录（立即拉各家 /v1/models）、重置当日额度、热重载配置文件（手动编辑 yaml 后点一下生效）。

---

## 三、日志文件

| 文件 | 内容 |
|---|---|
| `logs/gateway.log` | **每次请求一行**（含失败尝试）：`req model=xxx provider=yyy/zzz ttft=..ms total=..ms tokens=a+b status=OK attempts=1 err=-`。按天滚动，保留 30 天 |
| `logs/app.log` | 应用日志（启动、路由、熔断、目录刷新等）。保留 14 天 |

排查示例：`grep "status=FAILED" logs/gateway.log`、`grep "model=free-fast" logs/gateway.log`。

目录位置由 `GW_LOG_DIR` 决定（`scripts\start.bat` 会指到项目根的 `logs\`）；不带脚本直接 `java -jar` 时写工作目录下的 `logs\`。`scripts\status.bat log` 直接看 app.log 末尾。

---

## 四、配置文件说明（config/config.yaml）

管理台的所有增删改都写回这个文件；也可以直接手工编辑，编辑完在管理台「系统 → 热重载配置文件」即可生效（不用重启）。

```yaml
server: { bind: 127.0.0.1, port: 8317 }   # 只决定启动日志打什么；实际监听见第一节末尾那条坑
gateway-keys: ["<你的网关 Key>"]     # 本地鉴权 Key（可配多个），自己定强随机值，别用示例值
strategy: auto                          # priority/latency/weighted/auto/cost-first/free-only/paid-only

providers:                              # 供应商列表
  - id: deepseek                        # 唯一 ID
    name: DeepSeek 官方                  # 显示名
    baseUrl: https://api.deepseek.com/v1
    protocol: openai                    # openai=直通 | anthropic=自动协议转换
    priority: 5                         # 基础优先级（小者优先）
    defaultCostTier: low                 # free/low/standard/premium
    dailyTokenLimit: 1000000            # 可选：整家日预算
    channels:                           # Key 池（多 Key 轮转）
      - { id: main, apiKey: "sk-xxx", weight: 1 }
    static-models: [deepseek-chat]       # 可选：/v1/models 拉不到时兜底

routing-overrides:                       # 模型粒度覆盖
  - { providerId: zhipu, model: glm-4-flash, priority: 0, costTier: free, dailyTokenLimit: 1000000 }
  - { providerId: amd, model: qwen3.8-flash-next, alias: Qwen3.8-Flash-Next }  # 别名还原真实名

groups:                                  # 虚拟模型
  - name: free-fast
    members:
      - { providerId: tokenrouter, model: z-ai/glm-5.3-free, priority: 0 }
```

超时/熔断参数在 `defaults:` 段（connectTimeoutMs 3s / channelBudgetMs 30s / ttftTimeoutMs 10s / streamHeartbeatTimeoutMs 30s（首字前）/ firstTokenHeartbeatTimeoutMs 120s（已出 token 后，长思考模型流内事件间隔可达 60~100s）/ totalBudgetMs 120s / maxAttempts 3 / 熔断阈值3次 60s指数退避封顶30min / rateLimitCooldownMs 30s），一般不用动。

---

## 五、维护手册

### 日常
- **加一家新服务商**：管理台「+ 新增供应商」→ 填 baseUrl + Key → 自动拉目录 → Agent 立即可用。无需重启。
- **某家 Key 换了/过期**：供应商「编辑」→ 填新 apiKey（留空不改）。
- **某家限流/挂了**：什么都不用做——429 自动冷却 30s、连挂 3 次熔断，流量自动切到下一优先级；恢复后半开探测自动回切。管理台「系统」页可看熔断状态。
- **想强制走某家**：管理台把它优先级调小，或 Agent 请求 `供应商ID/模型名`。
- **额度烧太快**：给该家的模型加 routing 条目 `dailyTokenLimit`，当日用完自动摘除，次日零点自动恢复。

### 数据
- SQLite 库：用 `scripts\start.bat` 起就在 `<项目根>\data\llm-gateway.db`；不带脚本直接 `java -jar` 时回到默认 `config/data/llm-gateway.db`（`GW_DB_PATH` 留空即用默认）。删掉即清空全部统计（请求日志/用量/熔断快照）。
- 备份 = 备份 `config\config.yaml` + 那个 `.db` 文件（停服后备份，或先跑一次 `PRAGMA wal_checkpoint(TRUNCATE)`）。
- 管理台写入是原子替换（先写 .tmp 再改名），万一写坏可从 `config.yaml.tmp` 手工恢复。

### 常见问题
| 现象 | 处理 |
|---|---|
| Agent 报 401 | base_url 里的 Key 没对上 `gateway-keys` |
| Agent 报 404 model_not_found | 模型名不存在或目录还没拉到（首次启动后几秒），管理台点「刷新模型目录」 |
| 全部候选 503 no_candidate | 全被熔断/停用/预算耗尽；「系统」页看运行时状态，等冷却或点「重置当日额度」 |
| 供应商加了但不出现 | id 重复或 baseUrl 不合法，看返回的 error |
| 重启后管理台的改动丢了 | 不应该——所有改动都持久化；检查 config.yaml 是否被杀软回滚 |

---

## 六、管理 REST API（管理台就是调这些）

| 接口 | 说明 |
|---|---|
| GET /api/overview | 全量状态（供应商含模型清单/路由/分组/运行时/今日用量） |
| POST /api/providers | 新增供应商 `{id,name,baseUrl,protocol,priority,costTier,apiKey}` |
| PUT /api/providers/{id} | 修改供应商（部分字段，apiKey 留空不改） |
| DELETE /api/providers/{id} | 删除供应商（级联清理模型/路由/分组引用） |
| POST /api/providers/{id}/enable·disable | 启停 |
| POST /api/providers/{pid}/models | 添加模型 `{model,priority,costTier,dailyTokenLimit,alias}` |
| PUT /api/providers/{pid}/models/{model} | 修改模型 `{newModel,priority,costTier,dailyTokenLimit,alias}`（改名三处同步） |
| DELETE /api/providers/{pid}/models/{model} | 删除模型（级联清理路由/分组引用） |
| POST /api/providers/{pid}/channels | 加渠道 `{apiKey,weight}` |
| PUT /api/providers/{pid}/channels/{cid} | 改渠道 |
| DELETE /api/providers/{pid}/channels/{cid} | 删渠道 |
| POST /api/routing | 增改路由条目 `{providerId,model,priority,costTier,dailyTokenLimit,alias}` |
| DELETE /api/routing/{pid}/{model} | 删路由条目 |
| POST /api/groups / DELETE /api/groups/{name} | 分组增删改 |
| GET /api/usage?days=7 | 用量聚合（供应商×模型 + 日趋势） |
| GET /api/logs?limit=100&model= | 请求日志 |
| GET·PUT /api/strategy | 策略查看/切换 |
| POST /api/budget/reset | 重置当日额度 |
| POST /api/catalog/refresh | 刷新模型目录 |
| POST /api/config/reload | 热重载配置文件 |

**对外协议端点**（Agent 侧）：

| 端点 | 协议 | 鉴权头 |
|---|---|---|
| POST /v1/chat/completions · GET /v1/models | OpenAI 兼容 | `Authorization: Bearer <key>` |
| POST /v1/messages | **Anthropic 原生**（Claude Code 直连） | `x-api-key: <key>` 或 Bearer |

---

## 七、目录结构

```
项目根（= 仓库根，有 pom.xml 的那一层）/
├── pom.xml
├── scripts/                 # 已发布脚本：_env / doctor / start / stop / restart / status / build
├── config/
│   ├── config.example.yaml  # 模板（无密钥）
│   └── config.yaml          # 运行配置（含真实 Key，勿外传/git）
├── data/                    # SQLite 库（脚本默认落这里，不入库；不带脚本时在 config/data/）
├── logs/                    # gateway.log（请求日志）+ app.log（应用日志）
├── docs/                    # 部署指南 / 管理页使用指南 / 目录结构说明
├── target/                  # 构建产物 llmGateway.jar（不入库）
├── src/main/resources/
│   ├── application.yaml     # 真正决定端口/网卡、库位置、配置路径（改端口改这里）
│   ├── logback-spring.xml   # 日志：GW_LOG_DIR 可覆盖，默认工作目录下 logs/
│   └── static/ui/           # 管理台（单页，打包进 jar）
└── src/main/java/com/gw/
    ├── gateway/    # 接入层：鉴权、请求解析、SSE 写出与保活、错误模型
    ├── router/     # 路由：模型名→候选（直接/前缀点名/组展开）
    ├── policy/     # 策略：评分/EWMA/熔断/429/额度/并发/抽签分摊/决策留痕（状态唯一权威）
    ├── forward/    # 转发：超时预算、attempt 循环、多 Key、上下文压缩、对冲
    ├── provider/   # 适配器：OpenAI 直通 + Anthropic 双向转换（含流式）
    ├── catalog/    # 目录：后台定时拉 /v1/models、余额探测
    ├── stats/      # 统计：SQLite 异步落库 + gateway.log 请求日志
    ├── config/     # 配置：YAML→校验→快照；ConfigWriter 持久化写回
    ├── store/      # SQLite：WAL、异步批量写、快照恢复
    ├── probe/      # 对账：模型可用性探测与自动停用/恢复
    ├── util/       # ProviderClients：三条出站路径共用的代理化 HttpClient 工厂
    └── admin/      # 管理 REST（供应商/渠道/路由/分组 CRUD + 用量聚合）
```

`tools/`（便携 JDK+Maven）**不在仓库里**：脚本会顺带看一眼 `<项目根>\tools\jdk-*` 和 `tools\apache-maven-*`，你把便携工具链放进去就自动用，不放就用 `JAVA_HOME`/PATH 或 `GW_JDK`/`GW_MVN`（见第一节）。

## 八、核心机制速查

- **评分选路**：Score = 优先级 × 健康度 × 速度(EWMA TTFT) × 费用档；`auto` 策略下免费档先用，贵的兜底。
- **费用档硬过滤**：`free-only`/`paid-only` 在对侧档进候选池前整批剔除——熔断兜底、付费保底、点名请求都绕不过；免费全挂回 503（用户明令允许失败），这是"永不返回空"铁律唯一的例外。
- **failover 铁律**：仅首 token 前失败才换渠道；已开始输出不切换（防重复输出）。
- **五层超时**：TCP 3s → 渠道总预算 30s → TTFT 10s → 流中真内容窗口（首字前 30s / 出 token 后 120s，keep-alive 注释行不算续命）→ 请求总预算 120s。
- **熔断**：连续 3 次失败 OPEN，60s 指数退避封顶 30 分钟，半开单请求探测恢复。
- **429**：Key 级冷却（不连坐同家其他 Key）。
- **额度治理**：条目/提供商/Key 三层日预算，次日零点自动重置；`X-Gateway-Profile: critical` 头豁免。
- **协议适配**：OpenAI 兼容直通；Anthropic 双向转换（system/tools/流式事件/usage 全映射）。

## 九、已知限制（v2）

- Key 明文存 config.yaml（本地单机工具，文件自管；管理页显示时只留首尾字符）。
- 流式 TTFT 超时掐断后不换渠道（首 token 前换渠道覆盖 TCP/响应头/5xx/429）。
- 渠道并发上限已实现，但管理 UI 未暴露（可通过 routing 条目的 concurrencyLimit 配置）。

## 十、文档索引（docs/）

| 文档 | 内容 |
|---|---|
| `部署指南.md` | 从零跑起来：仓库布局、脚本的路径规则与七个覆盖变量、端口那条坑、配置字段全解、代理与出站、常见故障 |
| `管理页使用指南.md` | 管理台每个页面/每个字段怎么用 |
| `目录结构说明.md` | 前半：仓库布局与已发布脚本；后半：作者本机的工作区分区与整理史（那部分不在仓库里） |

本仓库只发布**部署、使用与目录结构**三份文档；架构设计、历轮改造计划与验收记录属于开发过程稿，不随仓库发布，代码本身即是最权威的实现说明。
