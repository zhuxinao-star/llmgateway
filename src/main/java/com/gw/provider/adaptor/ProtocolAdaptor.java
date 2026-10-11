package com.gw.provider.adaptor;

import java.util.List;
import java.util.Map;

/**
 * 协议适配器 SPI：网关统一用 OpenAI 格式处理请求/响应，
 * 适配器负责 OpenAI ⇄ 原生协议的双向转换（含 SSE 事件流）。
 */
public interface ProtocolAdaptor {

    /** 协议名，对应配置 provider.protocol */
    String protocol();

    /** OpenAI 请求体 → 原生上游请求体（真实模型名替换后调用） */
    Map<String, Object> transformRequest(Map<String, Object> openAiRequest, String realModel);

    /** 构造上游 HTTP 请求头（鉴权头由 forward 层补充） */
    Map<String, String> upstreamHeaders();

    /** 上游非流式响应体 → OpenAI 响应体 */
    Map<String, Object> transformResponse(Map<String, Object> upstreamBody);

    /**
     * 上游 SSE 事件行 → OpenAI SSE 事件行列表（0..n 行）。
     * 适配器保持自身状态（如 Anthropic 事件拼接），实例按请求创建。
     * @param upstreamEventLine 上游一行（不含换行，data: 前缀已剥或原样——见实现约定）
     * @return 待写给客户端的完整行列表（可为空）
     */
    List<String> transformStreamEvent(String upstreamEventLine);

    /** 流结束时可能需要补的收尾事件（如 OpenAI 的 [DONE]，Anthropic 的 message_stop 转换） */
    default List<String> streamFinish() { return List.of(); }

    /** 从流式/非流式响应中提取 usage（tokens in/out）；无则返回 null */
    default Map<String, Object> extractUsage(Object lastEventData) { return null; }

    /** 非 OpenAI 协议：流式聚合为整包响应（上游只回流式时） */
    default Map<String, Object> aggregateStream(List<String> openAiEventLines) { return null; }

    /**
     * 上游一行是否已是"真首字"（产出实际内容，而非 keep-alive/role-only/空 delta 事件）。
     * ForwardEngine 用它在 TTFT 预算内判定首字——此前 `!isBlank()` 会把 `: keep-alive` 注释行、
     * `data:{"choices":[{"delta":{"role":"assistant"}}]}` 这类空事件误判成"已出首字"，导致慢模型
     * （首字 100~230s）也能通过 TTFT 检查进入流式阶段、进而霸占并发位无法 failover。
     * 默认保守实现（非空非注释行即算，回退旧行为）；OpenAI/Anthropic 各自覆写精确判定。
     */
    default boolean isRealFirstToken(String upstreamLine) {
        return upstreamLine != null && !upstreamLine.isBlank() && !upstreamLine.startsWith(":");
    }

    /**
     * 判断一行 OpenAI 格式的 `data:` SSE 事件是否包含"真内容"——delta 里有非空 content、
     * reasoning_content（思考流）或 tool_calls。
     * 空 delta/role-only 事件不算真内容，不能续命心跳窗口。
     * reasoning_content 计入是修复过的坑：推理型免费模型（agnes 等）先流半天思考再出正文，
     * 若不认思考流，TTFT 闸门会把"正在干活"的模型判成"首字超时"误换家（19 条首字超时实锤）。
     */
    static boolean openAiDataHasContent(String dataLine) {
        if (dataLine == null || !dataLine.startsWith("data:")) return false;
        String payload = dataLine.substring(5).strip();
        if (payload.isEmpty() || "[DONE]".equals(payload)) return false;
        try {
            Map<String, Object> ev = Json.parseObject(payload);
            List<Object> chs = Json.asList(ev.get("choices"));
            if (chs == null || chs.isEmpty()) return false;
            Map<String, Object> ch = Json.asMap(chs.get(0));
            if (ch == null) return false;
            Map<String, Object> delta = Json.asMap(ch.get("delta"));
            if (delta == null) return false;
            if (delta.get("content") instanceof String t && !t.isBlank()) return true;
            if (delta.get("reasoning_content") instanceof String t && !t.isBlank()) return true;
            if (delta.get("reasoning") instanceof String t && !t.isBlank()) return true;
            return delta.get("tool_calls") != null;
        } catch (Exception e) {
            return false;
        }
    }
}
