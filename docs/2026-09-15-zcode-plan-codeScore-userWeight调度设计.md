## 目标
让代码开发场景下，代码能力强的模型获得更多调度机会，并让用户能自主调整权重，同时按"能力/成本"自动权衡性价比。

## 核心改动

### 1. 新增两个可配置字段（全链路贯通）
- `GatewayConfig.java`：RoutingOverride 加 `Double codeScore`（代码能力分 0-10）、`Double userWeight`（用户权重 0.1-10）+ getter/setter。
- `ConfigService.java`：加 `effectiveCodeScore(pid,model)`、`effectiveUserWeight(pid,model)`（照 effectiveCategory 模式，无值返回 null）。
- `ConfigWriter.java`：routing-overrides 手动序列化补两个字段（第 100 行后）。
- `AdminController.java`：`addModel`/`doUpdateModel` 两端的 if 判断与 setter 补两个字段；新增 `doubleOrNull` 工具方法；`overview` 补两行 `mv.put`。
- `index.html`：`openModelForm` 加两个数字输入框（f_codeScore 0-10、f_userWeight 0.1-10）；`saveModel` 补两行 body 收集。

### 2. 评分公式接入（PolicyService.score 内，K 因子之后新增）
- `codeScoreFactor`：仅 requestCategory=="code" 且模型有 codeScore 时生效，映射 `1.0 + codeScore/10.0`（0→1.0，10→2.0）；其余情况 1.0 中性。
- `userWeightFactor`：直接 userWeight 值（null→1.0 中性）。
- 性价比（能力/成本）：对 code 请求用 `(1 + codeScore/10) / cBase`（cBase 复用现有费用映射 free=1.0/low=0.8/standard=0.45/premium=0.1），能力越高越便宜分越高、贵则打折。非 code 请求等价现有 C 因子行为（不改变现状）。
- 三个新因子在最终连乘 `p*h*s*t*cFactor*k*r*limP` 里乘入。

### 3. 初始分值预填（用户可后改）
基于公开共识+命名映射给标准型号填 codeScore（qwen3.8=8.5、deepseek-v4=8.0、glm-5=7.5、glm-4=7.0、MiniCPM5-2B=6.0、agnes/sensenova 私有=6.5 推断），写进 config.yaml routing-overrides；image/embedding/doc 类不填。userWeight 默认不填（1.0 中性）。

### 4. 验证
编译重启 → code 请求确认高分模型前移 → 非 code 请求行为不变 → 管理页可编辑两字段且存盘落 config.yaml → 高价模型填高分确认性价比打折。

## 关键保证
所有新因子对"未打分/未设权重/非 code 请求"都返回中性 1.0，**完全不影响现有调度行为**（agnes-2.5 优先等现状保持不变），只在 code 场景 + 主动配置后生效。

## 涉及文件
后端 4 个 Java（GatewayConfig/ConfigService/ConfigWriter/AdminController/PolicyService）+ 前端 index.html + config.yaml，无架构变更。