# 工具访问网关无数据返回 — 根因分析与修复计划

- 分析时间：2026-09-17 10:45
- 分析人：AgnesCode
- 涉及版本：llmGateway 当前主分支（config/config.yaml + src/main/java）
- 数据来源：`logs/gateway.log`、`logs/app.log`、源码

---

## 一、问题现象

**用户描述**：通过工具（agent / IDE 插件 / CLI 工具）访问本地网关后，基本没有数据返回到工具。

**实际观察到的现象**：
- 工具侧表现为"卡住无响应"或"响应为空"。
- 网关侧对应请求要么被掐断，要么被失败逻辑吞掉，客户端收不到有效响应。

---

## 二、日志证据

### 2.1 请求来源分布

今天（2026-09-17）`logs/gateway.log` 所有非 `client=probe`（内置健康探测）的真实用户请求，只有：

```
client=deepseek-harness/0.1.5-r
```

- 未发现 `zcode` / `claude-code` / `curl` / `browser` 等工具的请求记录；
- 这意味着当前真实工具流量只来自 `deepseek-harness`。

### 2.2 故障类型集中

今天 `deepseek-harness` 发出的请求全部落在以下两类失败：

1. `status=STREAM_TIMEOUT`，`err=heartbeat timeout`
2. `status=FAILED`，`err=首字超时(>10000ms)`

典型日志行：

```
2026-09-17 09:56:05.271|req client=deepseek-harness/0.1.5-r model=free-fast provider=agnes/agnes-3.0-flash ttft=654ms total=120014ms tokens=0+0 status=STREAM_TIMEOUT attempts=1 profile=- err=heartbeat timeout compress=-
2026-09-17 09:58:42.526|req client=deepseek-harness/0.1.5-r model=free-fast provider=amd/DeepSeek-V4-Flash ttft=-ms total=-ms tokens=0+0 status=FAILED attempts=1 profile=- err=首字超时(>10000ms) compress=skipped
2026-09-17 10:00:50.087|req client=deepseek-harness/0.1.5-r model=free-fast provider=sensenova/deepseek-v4-flash ttft=3870ms total=121183ms tokens=0+0 status=STREAM_TIMEOUT attempts=2 profile=- err=heartbeat timeout compress=-
```

关键特征：
- `tokens=0+0`：没有真实输出落到客户端；
- `total≈120000ms`：基本精准命中 `totalBudgetMs=120000`；
- 失败上游集中在 `agnes/agnes-3.0-flash` 与部分 `amd`、`sensenova`。

### 2.3 对比：probe 正常

`client=probe` 的小样本探测基本稳定，说明：
- 网关本体启动正常、路由与鉴权正常；
- 问题是特定流量路径下的行为异常，不是网关整体挂掉。

---

## 三、根因分析

### 根因 1：流式心跳判定未识别 reasoning_content，导致长思考请求被误杀

**位置**：
- `ForwardEngine.java` 中 TTFT 首字判定
- `GatewayController.java` / `AnthropicController.java` 流式心跳判定
- `OpenAiAdaptor` / `ProtocolAdaptor` 中 `isRealFirstToken` / `openAiDataHasContent` 类判定

**当前逻辑**：
- 只把 `delta.content` 非空字符串视为“首字/真内容”；
- 把 `delta.reasoning_content`、`delta.tool_calls` 等事件当 keep-alive/空事件处理；
- 流式看门狗只看“真内容行”来刷新心跳窗口。

**问题场景**：
对 deepseek / glm / qwen 等支持 thinking 的模型：
- 真正的第一波输出可能是 `reasoning_content`（思考过程）；
- `content` 要很晚才出现；
- 网关因为看不到 `content`，认为在“空转/死流续命”；
- 最终在 `firstTokenHeartbeatTimeoutMs=120s` 或 `streamHeartbeatTimeoutMs=30s` 阈值处掐断。

**对应现象**：
- `ttft` 有时有值（说明上游真的出了首字，但可能是 reasoning_content）；
- `total` 精准卡在 120s；
- `tokens=0+0`；
- `err=heartbeat timeout`。

这是今天工具侧“无返回”的**直接根因**。

---

### 根因 2：`free-fast` 组对 deepseek-harness 的选路落到不稳定上游

**现象**：
- 所有 `deepseek-harness` 失败请求的模型都是 `free-fast`；
- 失败 provider 大量命中 `agnes/agnes-3.0-flash`；
- 同模型换上游（如 `sensenova/deepseek-v4-flash`）在 probe 里是稳定的。

**结论**：
`free-fast` 组的默认路径把这类请求路由到了当前表现不稳的免费上游，放大了根因 1 的影响。

---

### 根因 3：Client 并发排队加重“卡住”体感

**配置**：
```yaml
clientMaxConcurrency: 2
clientMaxConcurrencyWaitMs: 30000
```

**影响**：
- `deepseek-harness` 如果一次性发出多轮对话请求，第 3 个请求要排队；
- 排队上限 30s，再加上后续转发最长 120s；
- 工具侧很容易看到“2~3 分钟没动静”。

这条不是主因，但会显著放大用户的“无响应”体感。

---

### 补充观察：其他工具没有请求记录

日志中未出现 `zcode` / `claude-code` / 浏览器 / curl 等 client。

两种可能：
1. 这些工具未正确配置 `base_url = http://127.0.0.1:8317`；
2. 或者这些工具请求了，但连接被 OS/杀毒/防火墙拦截，未到达网关。

这一层不在本次“网关侧无返回”的分析范围内，但会影响最终工具体验。

---

## 四、修复方案

### 方案一（核心修复，改代码）

**目标**：让网关承认 `reasoning_content` 也是有效输出，不再误杀长思考流。

**改动点**：
1. `OpenAiAdaptor.isRealFirstToken(...)`
   - 当前：只认 `delta.content` 非空
   - 改为：`delta.content` 非空 **或** `delta.reasoning_content` 非空，都算首字

2. 流式心跳判定（`GatewayController.streamToClient`、`AnthropicController.streamToClient`、`OpenAiAdaptor.transformStreamEvent` 内）
   - 当前：只有 `delta.content` 非空才刷新 `lastRealAt`
   - 改为：`delta.content` 非空 **或** `delta.reasoning_content` 非空，都刷新窗口

**副作用**：
- 对非 thinking 模型，`reasoning_content` 字段为空，行为不变；
- 对 truly dead 流，仍然会被 `firstTokenHeartbeatTimeoutMs` 兜底掐断。

**收益**：
- 直接解决 `deepseek-harness` 的 `STREAM_TIMEOUT` 问题；
- 顺带修复 claude-code / zcode 未来接入 thinking 模型时的同类问题。

---

### 方案二（配置修复，立刻见效）

**目标**：避免 `free-fast` 当前把 deepseek 类请求路由到不稳定的 agnes。

**建议调整**（在 `config/config.yaml` 中）：
1. 降低 `agnes/agnes-3.0-flash` 在 `free-fast` 中的优先级；
2. 提升 `sensenova/deepseek-v4-flash` 或 `amd/qwen3.8-flash-next` 为优先；
3. 或给 `agnes/agnes-3.0-flash` 加 `excludeFromAutoAll: true`，让它不在自动组里。

**生效方式**：
- 管理台 → 系统 → **热重载配置文件**，无需重启。

---

### 方案三（配置修复，降排队体感）

**建议调整**：
```yaml
defaults:
  clientMaxConcurrency: 5
  clientMaxConcurrencyWaitMs: 10000
```

**效果**：
- 同一工具最多 5 并发；
- 排队最多 10s 就放行撞墙，而不是 30s；
- 不破坏“防一个工具打爆免费上游”的初衷。

---

### 方案四（可观测增强）

1. 请求日志新增字段：
   - `first_token_type=reasoning|content|none`
   - `stream_killed_by=ttft|heartbeat|total_budget|abort`
2. 管理台“系统”页加“本机连通性自检”：
   - 自检网关自身端口 / key / 主要上游可达性；
3. `gateway.log` 对 `compress=skipped` 的大上下文请求单独打标签。

---

## 五、实施计划

### 阶段一（不改代码，立刻缓解）

| # | 动作 | 文件 / 入口 | 目的 |
|---|---|---|---|
| 1 | 调整 `free-fast` 组优先级，减少 `agnes-3.0-flash` 命中率 | `config/config.yaml` | 避开当前不稳上游 |
| 2 | 调大 `clientMaxConcurrency`、缩短 `clientMaxConcurrencyWaitMs` | `config/config.yaml` | 工具不再长时间排队 |
| 3 | 管理台 → 系统 → **热重载配置文件** | UI | 配置立即生效 |

预计效果：`deepseek-harness` 的失败率明显下降，工具体感“没那么卡”。

---

### 阶段二（改代码，根治）

| # | 改动 | 文件 | 测试方式 |
|---|---|---|---|
| 4 | `isRealFirstToken` 识别 `reasoning_content` | `OpenAiAdaptor` | mock 带 reasoning 的 SSE |
| 5 | 流式心跳判定识别 `reasoning_content` | `GatewayController` / `AnthropicController` | 长思考请求不复现 STREAM_TIMEOUT |
| 6 | 同步修复 Anthropic 路径（如需要） | `AnthropicAdaptor` | claude-code 同样场景 |
| 7 | 新增 `first_token_type` 日志字段 | `StatsService` / `gateway.log` | 方便后续排查 |

---

### 阶段三（可选增强）

| # | 动作 | 目的 |
|---|---|---|
| 8 | 管理台加“本机连通性自检” | 快速判断工具连不上网关 vs 网关内部失败 |
| 9 | 优化大上下文路径的超时 / 失败提示 | 避免 120s 后才报错 |

---

## 六、结论

1. **今天真实工具只来自 `deepseek-harness/0.1.5-r`**，其他工具未产生网关侧请求记录。
2. 主要失败形态是 `STREAM_TIMEOUT`，且集中在 `model=free-fast`、上游 `agnes/agnes-3.0-flash`。
3. **主因**：网关把 thinking 模型的 `reasoning_content` 当作“空内容”，导致长思考请求被心跳机制误杀。
4. **次因**：`free-fast` 当前把这类请求优先路由到不稳定的免费上游。
5. **次次因**：`clientMaxConcurrencyWaitMs=30000` 让排队体感很卡。
6. **优先动作**：先改配置（阶段一），再改代码（阶段二）。

---

## 七、后续验证建议

1. 修改配置并热重载后，让 `deepseek-harness` 重跑一轮；
2. 观察 `gateway.log` 中：
   - 是否还有 `deepseek-harness` + `STREAM_TIMEOUT`；
   - 上游是否从 `agnes/agnes-3.0-flash` 切换到 `sensenova` / `amd`；
   - `tokens` 是否从 `0+0` 变为正常值。
3. 若根因 1 仍然存在，再进入阶段二改代码。
4. 工具侧建议：
   - 确认 `base_url` 已指向 `http://127.0.0.1:8317`；
   - 确认 API key 与 `gateway-keys` 一致；
   - 必要时显式点名 `sensenova/deepseek-v4-flash` 等非 agnes 上游。
