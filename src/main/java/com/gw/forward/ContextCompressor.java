package com.gw.forward;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.policy.PolicyService;
import com.gw.policy.RequestCategoryAnalyzer;
import com.gw.provider.adaptor.Json;
import com.gw.router.RouterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 上下文自动压缩：请求输入超过阈值时，把历史消息压成摘要再转发。
 *
 * 动机：Agent 工具（DSH/claude-code）每轮携带全部会话历史，输入随轮次线性膨胀
 * （实测 DSH 一路涨到 164K token），导致——免费模型上下文超限 400、TPM 配额
 * 双倍烧、评分 TTFT 被大请求刷慢。压缩后请求小、快、且能塞进 128K 免费模型。
 *
 * 策略：
 * - system 消息全保留（工具系统提示是行为契约，不能丢）
 * - 最近 N 轮（keepRecentMessages 条）原样保留——模型近期记忆直接影响当前任务
 * - 更早的消息 → 压缩模型生成结构化摘要，替换为一条 system 消息注入
 * - tool 消息成组裁剪：裁剪边界落在 assistant(tool_calls)/tool 组中间会产生孤儿
 *   tool 消息（上游 400），压缩启动前先把消息序列按工具组边界重新分段
 *
 * 滚动缓存（性能关键）：Agent 历史是 append-only 的——上轮压过的前缀本轮必然
 * 原样重现。按客户端记录"已压到第几条 + 摘要"，前缀哈希校验命中后只压缩
 * 新增增量（几 K token，秒级）；未命中（换会话/系统提示变了）才全量压。
 * 没有这层缓存则每轮请求都要全量压缩一次（100K 输入 1~2 分钟），功能不可用。
 *
 * 铁律：压缩任何一步失败（压缩模型挂/返回空/超时）→ 原样转发原请求，
 * 压缩功能永远不能是请求失败的原因。 */
@Service
public class ContextCompressor {

    private static final Logger log = LoggerFactory.getLogger(ContextCompressor.class);

    /** 压缩提示词：面向"接着干活"设计——保留任务脉络、关键决定与未完成事项，丢弃过程性细节 */
    private static final String COMPRESS_PROMPT = """
            你是会话压缩器。下面是 AI 助手与用户的历史对话（含工具调用记录）。
            请把它压缩成一份给"下一轮对话"用的上下文摘要，让助手读了能无缝继续当前任务。
            必须保留：任务目标与背景、已完成的关键步骤及其结果、重要的文件路径/命令/配置值、
            未完成事项与下一步计划、用户明确的偏好或纠正。
            必须丢弃：寒暄、重复内容、大段代码/日志原文（只留结论与关键行）、无关的中间探索。
            直接输出摘要正文（不要标题不要客套话），中文，控制在 2000 字以内。""";

    /** 增量压缩提示词：在旧摘要基础上续压新消息 */
    private static final String ROLLING_PROMPT = """
            你是会话压缩器。第一部分是此前对话的旧摘要，第二部分是此后新增的对话（含工具调用记录）。
            请把两者合并成一份更新后的上下文摘要，供助手继续当前任务使用。
            必须保留：任务目标与背景、已完成的关键步骤及其结果、重要的文件路径/命令/配置值、
            未完成事项与下一步计划、用户明确的偏好或纠正。新增内容里的信息优先保留。
            直接输出合并后的摘要正文（不要标题不要客套话），中文，控制在 2000 字以内。""";

    /** 滚动状态：某客户端历史已压缩到的位置 + 已生成的摘要 */
    private static class RollingState {
        String prefixHash;   // 已压缩消息段（messages[0..segmentEnd)）的尾锚点哈希——校验新请求前缀一致
        String summary;      // 该段的摘要
        int segmentEnd;      // 摘要覆盖到第几条消息（不含）
        int msgCount;        // 上次压缩时会话总消息数（#12 单调性兜底：变少=rewind/新会话）
        volatile long lastUsedAt;
    }
    private final Map<String, RollingState> rolling = new ConcurrentHashMap<>();

    /** #12 前缀哈希的尾锚点窗口：只哈希截点前最后这么多条，避免每次对整段历史 O(n) 序列化 */
    private static final int HASH_ANCHOR_WINDOW = 8;

    private final ConfigService configService;
    private final ForwardEngine forwardEngine;
    private final RouterService routerService;
    private final com.gw.policy.PolicyService policyService;
    private final com.gw.stats.StatsService statsService;

    public ContextCompressor(ConfigService configService, ForwardEngine forwardEngine, RouterService routerService,
                             com.gw.policy.PolicyService policyService, com.gw.stats.StatsService statsService) {
        this.configService = configService;
        this.forwardEngine = forwardEngine;
        this.routerService = routerService;
        this.policyService = policyService;
        this.statsService = statsService;
    }

    /** 压缩结果：原样或压缩后的请求 + 元信息（attempt 循环里逐候选重压缩成本高，一次压缩全程复用）。
     *  compressTag：本次压缩走的路径（skipped/none-needed/full-ok/incremental-ok/hard-truncate），
     *  落 request_log 供管理页排查（不依赖易删的文件日志）。 */
    public record Compressed(Map<String, Object> request, boolean compressed, int tokensBefore, int tokensAfter,
                             String compressTag) {}

    /**
     * 按需压缩请求。输入未超阈值 / 关闭 / 压缩失败 → 原样返回（compressed=false）。
     * @param openAiRequest OpenAI 格式请求体（会被原地修改，调用方需传可变副本）
     * @param client 客户端标识（滚动缓存分桶键；null 视为 "unknown"）
     */
    public Compressed maybeCompress(Map<String, Object> openAiRequest, String client) {
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        int threshold = d.getContextCompressThresholdTokens();
        if (threshold <= 0) {
            log.debug("[压缩] 功能关闭(threshold={})，不压缩", threshold);
            return new Compressed(openAiRequest, false, 0, 0, "skipped");
        }

        String compressorModel = d.getContextCompressModel();
        if (compressorModel == null || compressorModel.isBlank()) {
            log.debug("[压缩] 未配置压缩模型，不压缩");
            return new Compressed(openAiRequest, false, 0, 0, "skipped");
        }

        int est = RequestCategoryAnalyzer.estimateInputTokens(openAiRequest);
        if (est <= threshold) {
            log.debug("[压缩] 输入 {} token 未超阈值 {} token，不压缩", est, threshold);
            return new Compressed(openAiRequest, false, est, est, "skipped");
        }

        List<Object> messages = Json.asList(openAiRequest.get("messages"));
        if (messages == null || messages.size() < 4) {
            log.debug("[压缩] 消息条数 {} < 4，样本不足不压缩", messages == null ? 0 : messages.size());
            return new Compressed(openAiRequest, false, est, est, "skipped");
        }
        log.info("[压缩] 触发：输入 {} token（阈值 {}）消息 {} 条 client={}", est, threshold, messages.size(), client);
        try {
            return doCompress(openAiRequest, messages, est, threshold, compressorModel, d, client);
        } catch (Exception e) {
            // 异常也走硬截断兜底，保证 token 一定下降
            int sysEnd = firstNonSystem(messages);
            int keepFrom = computeKeepFrom(messages, sysEnd, d);
            log.warn("[压缩] 异常走硬截断兜底: {}", e.getMessage());
            return hardTruncateFallback(openAiRequest, messages, sysEnd, keepFrom, est, d);
        }
    }

    /**
     * 保留段起点：条数上限与 token 预算双重约束，再按工具组边界对齐。
     * 从最后一条往前累加估算 token：keepRecentMessages 条封顶，且累计不超过
     * keepRecentTokens（0=不限）。至少保留 1 条（哪怕它本身超预算——0 条会让保留段空转）。
     * 动机：Agent 场景近期常有单条几万 token 的工具输出，按条数保留时"最近15条"
     * 就能吃掉 7 万 token，摘要再小也压不到阈值下（实测 91K→71K 仍超 60K）。
     */
    private int computeKeepFrom(List<Object> messages, int sysEnd, GatewayConfig.Defaults d) {
        int n = messages.size();
        int keepFrom = Math.max(sysEnd, n - d.getKeepRecentMessages());
        int budget = d.getKeepRecentTokens();
        if (budget > 0) {
            int acc = 0;
            int i = n;
            while (i > sysEnd + 1) {
                int t = RequestCategoryAnalyzer.estimateMessageTokens(Json.asMap(messages.get(i - 1)));
                if (i < n && acc + t > budget) break;   // 首条必进，之后的超预算就不往前扩
                acc += t;
                i--;
            }
            keepFrom = Math.min(keepFrom, i);
        }
        return alignToToolGroupBoundary(messages, keepFrom, sysEnd);
    }

    private Compressed doCompress(Map<String, Object> req, List<Object> messages, int est, int threshold,
                                 String compressorModel, GatewayConfig.Defaults d, String client) {
        // 1) 分段：开头连续 system 段 | 历史段 | 保留段（条数上限 + token 预算双约束，按工具组边界对齐）
        int sysEnd = firstNonSystem(messages);
        int n = messages.size();
        int keepFrom = computeKeepFrom(messages, sysEnd, d);
        if (keepFrom <= sysEnd + 1) return new Compressed(req, false, est, est, "none-needed");  // 历史段为空/仅1条，无需压

        // 2) 滚动缓存命中？前缀一致 → 只压增量；否则全量压。
        //    费用守卫：配置为组名（如 free-fast）→ 只允许免费候选参与压缩（免费政策
        //    变更/误标 free 时不产生账单）；配置为点名（provider/model）→ 视为用户
        //    显式授权该模型，付费也尊重
        boolean freeOnly = !compressorModel.contains("/");
        // #12 分桶 key = client + 会话指纹：只按 client 时同工具多会话（Claude Code 子代理/
        // 多窗口/rewind）互相覆盖前缀哈希，被迫频繁走全量压缩（串行遍历候选，最坏数分钟）。
        String cl = rollingKey(messages, client);
        RollingState st = rolling.get(cl);
        String summary;
        String path = "full";   // 走全量还是增量，用于最终 compressTag
        // 增量命中前提：会话指纹一致（已进 key）+ 消息数单调不减（无 rewind/换会话）+ 尾锚点前缀一致
        boolean sameSession = st != null && st.prefixHash != null
                && st.segmentEnd <= n && n >= st.msgCount
                && st.prefixHash.equals(hashOf(messages, st.segmentEnd));
        if (sameSession && st.segmentEnd < keepFrom) {
            path = "incremental";
            // 增量：旧摘要 + 上次截点到本次截点之间的新消息（含上次的"保留段"——这次它们成了历史）
            StringBuilder delta = new StringBuilder("【旧摘要】\n").append(st.summary)
                    .append("\n\n【此后新增的对话】\n");
            for (int i = st.segmentEnd; i < keepFrom; i++) {
                String line = roleLine(Json.asMap(messages.get(i)));
                if (!line.isEmpty()) delta.append(line).append('\n');
            }
            // 旧摘要是整段历史的唯一载体，绝不能被输入截断误删：把它作为"必须保留的前缀"传给
            // callCompressor，超窗口时只截新增段里更早的部分，旧摘要完整保留。
            String keepPrefix = "【旧摘要】\n" + st.summary + "\n\n【此后新增的对话】\n";
            summary = callCompressor(ROLLING_PROMPT, delta.toString(), compressorModel, d, freeOnly, keepPrefix);
            log.info("[压缩] 增量压缩：复用旧摘要（覆盖前 {} 条），新压 {} 条", st.segmentEnd, keepFrom - st.segmentEnd);
        } else {
            // 全量：历史段整体喂压缩模型（慢路径，首次、换会话指纹、rewind 或前缀校验失败时走）
            String reason;
            if (st == null) reason = "首次压缩无缓存";
            else if (st.prefixHash == null || st.segmentEnd > n || n < st.msgCount) reason = "会话轮次倒退(换会话/rewind)";
            else if (!st.prefixHash.equals(hashOf(messages, st.segmentEnd))) reason = "前缀哈希变更(历史被编辑)";
            else reason = "缓存截点未前进";
            log.info("[压缩] 全量压缩：{}，压历史段 {} 条", reason, keepFrom - sysEnd);
            StringBuilder histText = new StringBuilder();
            for (int i = sysEnd; i < keepFrom; i++) {
                String line = roleLine(Json.asMap(messages.get(i)));
                if (!line.isEmpty()) histText.append(line).append('\n');
            }
            summary = callCompressor(COMPRESS_PROMPT, histText.toString(), compressorModel, d, freeOnly, null);
        }
        if (summary == null) {
            // 压缩候选链穷尽/失败 → 硬截断兜底：保留 system 段 + 最近 keepRecentMessages 条，
            // 丢弃更早的历史段（无摘要），保证 token 一定下降，绝不原样把 100K+ 塞回上游。
            return hardTruncateFallback(req, messages, sysEnd, keepFrom, est, d);
        }

        // 3) 重组：system 段 + 压缩摘要（system 身份注入）+（必要时补回被摘要覆盖的最近 user 指令）+ 保留段
        List<Object> rebuilt = new ArrayList<>(messages.subList(0, sysEnd));
        Map<String, Object> summaryMsg = new LinkedHashMap<>();
        summaryMsg.put("role", "system");
        summaryMsg.put("content", "以下是本会话此前对话的压缩摘要（较早日志已压缩，最近对话原文保留在后面）：\n" + summary
                + "\n\n【重要】摘要里列出的只是已完成的步骤，任务本身仍在进行中——"
                + "请依据“未完成事项/下一步”与最近的对话原文继续推进（该调用工具就发起工具调用），"
                + "不要因为摘要内容详尽就提前收尾或只回一段总结。");
        rebuilt.add(summaryMsg);
        Map<String, Object> usr = userInstructionIfMissing(messages, keepFrom, n);
        if (usr != null) {
            rebuilt.add(usr);
            log.info("[压缩] 保留段无 user 消息，补回最近一条 user 指令（防上游 400 / 小模型早停）");
        }
        rebuilt.addAll(messages.subList(keepFrom, n));
        req.put("messages", rebuilt);

        // 4) 更新滚动状态（哈希覆盖到本次截点，下轮请求校验前缀用）
        RollingState ns = new RollingState();
        ns.prefixHash = hashOf(messages, keepFrom);
        ns.summary = summary;
        ns.segmentEnd = keepFrom;
        ns.msgCount = n;
        ns.lastUsedAt = System.currentTimeMillis();
        rolling.put(cl, ns);
        if (rolling.size() > 64) evictOldest();

        int after = RequestCategoryAnalyzer.estimateInputTokens(req);
        String tag = path + "-ok";
        // 5) 二次收缩：token 预算下保留段已尽量小，若仍超阈值（近期存在单条几万 token 的
        //    超大工具输出/文件内容），对保留段逐条截断超长 content——协议结构（tool_calls/
        //    消息配对）不动，只削文本，保证 token 尽量压回阈值内。
        if (after > threshold && d.getKeepRecentTokens() > 0) {
            int cap = Math.max(1500, d.getKeepRecentTokens() / 4);
            int keptStart = rebuilt.size() - (n - keepFrom);
            if (shrinkOversizedText(rebuilt, keptStart, cap)) {
                after = RequestCategoryAnalyzer.estimateInputTokens(req);
                tag = path + "-ok-shrunk";
            }
        }
        if (after > threshold) {
            log.warn("[压缩] 收缩后仍约 {}K > 阈值 {}K：超出部分为 system 提示词/tools 定义等固有开销，网关无法再压",
                    after / 1000, threshold / 1000);
        }
        log.info("[压缩] 完成：约 {}K → {}K token（阈值 {}K，保留最近 {} 条，压缩模型 {}）",
                est / 1000, after / 1000, threshold / 1000, n - keepFrom, compressorModel);
        return new Compressed(req, true, est, after, tag);
    }

    /**
     * 逐条截断 [from..end) 内超预算消息的文本内容（保留段专用）。
     * 只动 content（string 或 text 分段），不碰 tool_calls（JSON 结构截断即坏）与图片。
     * 头 70% + 尾 30% 保留：工具输出两头最关键（命令回显 + 结尾结论/报错）。
     * @return 是否实际截断过（用于重估与打标）
     */
    @SuppressWarnings("unchecked")
    private static boolean shrinkOversizedText(List<Object> msgs, int from, int capTokens) {
        boolean any = false;
        int targetChars = capTokens * 3;   // 混合文本粗折算（ASCII≈4 字/token，CJK≈2 字/token）
        for (int i = Math.max(from, 0); i < msgs.size(); i++) {
            Map<String, Object> m = Json.asMap(msgs.get(i));
            if (m == null) continue;
            if (RequestCategoryAnalyzer.estimateMessageTokens(m) <= capTokens) continue;
            Object c = m.get("content");
            if (c instanceof String s) {
                m.put("content", cutWithMark(s, targetChars));
                any = true;
            } else if (c instanceof List<?> parts) {
                // 多模态分段：text 分段按数量摊预算，非文本段（图片等）原样保留
                long textParts = parts.stream()
                        .map(Json::asMap).filter(p -> p != null && p.get("text") instanceof String).count();
                if (textParts == 0) continue;
                int per = Math.max(400, (int) (targetChars / textParts));
                List<Object> newParts = new ArrayList<>(parts.size());
                boolean changed = false;
                for (Object p : parts) {
                    Map<String, Object> pm = Json.asMap(p);
                    if (pm != null && pm.get("text") instanceof String s && s.length() > per) {
                        Map<String, Object> copy = new LinkedHashMap<>((Map<String, Object>) pm);
                        copy.put("text", cutWithMark(s, per));
                        newParts.add(copy);
                        changed = true;
                    } else newParts.add(p);
                }
                if (changed) {
                    m.put("content", newParts);
                    any = true;
                }
            }
        }
        return any;
    }

    /** 头尾保留、中间替换为截断标记（省略字符数写进标记，模型能意识到信息缺失） */
    private static String cutWithMark(String s, int targetChars) {
        if (s.length() <= targetChars) return s;
        int head = (int) (targetChars * 0.7);
        int tail = targetChars - head;
        return s.substring(0, head)
                + "\n……(网关截断：中间 " + (s.length() - targetChars)
                + " 字符因超出保留预算被省略，如需这段内容请重新执行相应工具调用)……\n"
                + s.substring(s.length() - tail);
    }

    /**
     * 压缩失败兜底硬截断：无摘要时，把消息裁成「system 段 + 最近 keepRecentMessages 条」，丢弃更早历史，
     * 保证 token 一定下降（绝不让 100K+ 原样打回上游）。keepRecentMessages 条以内的近期上下文原样保留，
     * 至少最近几轮对话不断档。
     */
    private Compressed hardTruncateFallback(Map<String, Object> req, List<Object> messages,
                                            int sysEnd, int keepFrom, int est, GatewayConfig.Defaults d) {
        int n = messages.size();
        if (keepFrom <= sysEnd + 1 || keepFrom >= n) {
            // 已无可截空间（历史段本身就只有 0~1 条）：退回原样，避免空消息
            return new Compressed(req, false, est, est, "skipped");
        }
        List<Object> rebuilt = new ArrayList<>(messages.subList(0, sysEnd));
        // 降级提示：硬截断是"丢历史"的有损兜底，插入一条 system 消息明示下游——
        // 更早历史因压缩失败被截断，避免模型在不知情时脑补/误判，也方便用户察觉。
        Map<String, Object> note = new LinkedHashMap<>();
        note.put("role", "system");
        note.put("content", "【网关提示】本会话更早的 " + (keepFrom - sysEnd)
                + " 条历史消息因上下文压缩失败被截断，当前仅保留最近 "
                + (n - keepFrom) + " 条原文；任务若仍未完成，请按最近原文继续推进（该调用工具就调用），"
                + "不要提前收尾；若后续任务依赖被截断的历史，请向用户确认或要求其重新提供关键信息。");
        rebuilt.add(note);
        Map<String, Object> usr = userInstructionIfMissing(messages, keepFrom, n);
        if (usr != null) rebuilt.add(usr);
        rebuilt.addAll(messages.subList(keepFrom, n));
        req.put("messages", rebuilt);
        int after = RequestCategoryAnalyzer.estimateInputTokens(req);
        String tag = "hard-truncate";
        int threshold = d.getContextCompressThresholdTokens();
        if (after > threshold && d.getKeepRecentTokens() > 0) {
            // 兜底路径同样可能撞上"少数超大消息"：保留段逐条截断文本，压回阈值内
            int cap = Math.max(1500, d.getKeepRecentTokens() / 4);
            if (shrinkOversizedText(rebuilt, rebuilt.size() - (n - keepFrom), cap)) {
                after = RequestCategoryAnalyzer.estimateInputTokens(req);
                tag = "hard-truncate-shrunk";
            }
        }
        log.warn("[压缩] 失败→硬截断兜底：约 {}K → {}K token（丢弃 {} 条历史），近期上下文保留 {} 条",
                est / 1000, after / 1000, keepFrom - sysEnd, n - keepFrom);
        return new Compressed(req, true, est, after, tag);
    }

    /**
     * 调压缩模型拿摘要；候选穷尽后仍无有效输出 → null（调用方降级原样转发）。
     *
     * 压缩候选链（不押单一模型——免费政策会变，收费/挂掉时全链 failover）：
     * 1. contextCompressModel 支持组名（free-fast）或点名（provider/model）——组名给全部免费候选
     * 2. 复用 PolicyService.rank：费用档过滤（默认只允许 free 压缩，policy 保证不花钱；
     *    显式配付费点名时尊重用户）、contextWindow 过滤（100K 历史不打 64K 模型）、
     *    熔断/冷却/多 Key 轮转全部继承——与主路由同套治理
     * 3. 输入截断保护：免费模型最大窗口 ~128K，历史段超 100K token 时从"最旧"截断
     *    （配合滚动缓存，截掉的部分下轮会进增量摘要，信息不丢只是晚一轮进摘要）
     * 4. 摘要质量门槛：太短（<200 字符≈没干活的敷衍输出）视为失败换下一家 */
    private String callCompressor(String prompt, String inputText, String compressorModel, GatewayConfig.Defaults d,
                                  boolean freeOnly, String keepPrefix) {
        return callCompressor(prompt, inputText, compressorModel, d, freeOnly, keepPrefix, COMPRESS_CLIENT);
    }

    /** #selfcheck 记账客户端参数化：真实压缩记 gw-compress（进预算），体检干跑记 selfcheck（隔离，不污染选举）。 */
    private String callCompressor(String prompt, String inputText, String compressorModel, GatewayConfig.Defaults d,
                                  boolean freeOnly, String keepPrefix, String recordClient) {
        Map<String, Object> compressReq = new LinkedHashMap<>();
        compressReq.put("model", compressorModel);
        compressReq.put("messages", List.of(
                Map.of("role", "system", "content", prompt),
                Map.of("role", "user", "content", inputText)
        ));
        compressReq.put("max_tokens", 3000);
        compressReq.put("stream", false);

        List<PolicyService.Candidate> all = routerService.resolve(compressorModel);
        if (all.isEmpty()) {
            log.warn("[压缩] 压缩模型 {} 不存在，原样转发", compressorModel);
            return null;
        }
        // rank 排序 + 治理过滤（contextWindow 超限剔除、熔断/冷却降权、多 Key 轮转）。
        // 传 requestCategory=null（摘要生成是通用任务不吃语义加分）。
        int estCompIn = RequestCategoryAnalyzer.estimateInputTokens(compressReq);
        List<PolicyService.Scored> ranked = policyService.rank(all, configService.snapshot().config.getStrategy(),
                false, null, estCompIn);
        if (ranked.isEmpty()) {
            log.warn("[压缩] 压缩候选全部不可用（冷却/超窗口），原样转发");
            return null;
        }
        // 输入截断：按首个候选的实际窗口留 20% 余量，超限截最旧历史
        //（滚动缓存下截断只影响本次输入，摘要仍会覆盖全段）
        int firstCw = configService.snapshot().effectiveContextWindow(
                ranked.get(0).candidate.providerId(), ranked.get(0).candidate.model()) == null ? 100_000
                : configService.snapshot().effectiveContextWindow(
                ranked.get(0).candidate.providerId(), ranked.get(0).candidate.model());
        int maxIn = (int) (firstCw * 0.8);
        if (estCompIn > maxIn && inputText.length() > maxIn * 4) {
            if (keepPrefix != null && inputText.startsWith(keepPrefix)) {
                // 增量：旧摘要是历史唯一载体，绝不能被截。先把旧摘要段完整保留，
                // 再在"新增段"内部从更早部分往后截，只牺牲新增里离当前任务较远的中间段。
                int prefixLen = keepPrefix.length();
                String rest = inputText.substring(prefixLen);          // 新增段
                int tailBudget = maxIn * 4 - Math.min(prefixLen, maxIn * 4);
                if (rest.length() > tailBudget) {
                    inputText = keepPrefix
                            + "（注：新增对话中更早的部分已达窗口上限被截断）\n"
                            + rest.substring(rest.length() - tailBudget);
                    log.warn("[压缩] 压缩输入约 {}K token 超首位候选窗口 {}K，增量模式保留旧摘要，截新增段更早部分",
                            estCompIn / 1000, firstCw / 1000);
                }
            } else {
                inputText = "（注：更早历史已达窗口上限被截断）\n" + inputText.substring(inputText.length() - maxIn * 4);
                log.warn("[压缩] 压缩输入约 {}K token 超首位候选窗口 {}K，已截断最旧历史", estCompIn / 1000, firstCw / 1000);
            }
            // 截断后重估压缩请求体（截断后的 inputText 才是真实输入）
            compressReq.put("messages", List.of(
                    Map.of("role", "system", "content", prompt),
                    Map.of("role", "user", "content", inputText)));
        }

        // 压缩调用超时（交互式硬死线，修复"压缩卡 113s 拖断客户端"）：非流式压缩过去单条预算恒 120s 且整轮
        // 候选链无封顶，免费上游(agnes)一挂就吃满 113s 才吐垃圾 → 同步阻塞用户那一轮 → Claude Code 主动断开
        // （"问题没答完就结束"）。现双重封顶：单条 ≤ compressCallTimeoutMs，整轮墙钟 ≤ compressOverallBudgetMs；
        // 预算耗尽即停 failover，summary 保持 null → 落既有 hardTruncateFallback（丢更老历史、token 必降、绝不
        // 把 100K+ 原样塞回上游）。用户那一轮最多等 ~整轮上限即开始拿到回答，远优于死等到被客户端掐断。
        long compLoopStart = System.currentTimeMillis();
        long perCallCap = Math.max(5_000, d.getCompressCallTimeoutMs());
        long overallDeadline = compLoopStart + Math.max(perCallCap, d.getCompressOverallBudgetMs());
        String summary = null;
        String usedTier = null;
        int idx = 0;   // 候选尝试序号（记账 attempts）
        for (PolicyService.Scored s : ranked) {
            idx++;
            // 费用守卫：免费-only 模式下跳过任何非 free 候选（点名付费模型时用户已显式授权）
            String tier = configService.snapshot().effectiveCostTier(
                    s.candidate.providerId(), s.candidate.model());
            if (freeOnly && !"free".equalsIgnoreCase(tier)) {
                log.info("压缩候选 {}/{} 非免费档（{}），跳过", s.candidate.providerId(), s.candidate.model(), tier);
                continue;
            }
            // 本轮预算：单条上限与整轮剩余取小；剩余不足 floor 则停止 failover、走降级（绝不无限等挂死的免费家）
            int budget = nextCompressBudgetMs(overallDeadline, perCallCap, System.currentTimeMillis(), 5_000);
            if (budget <= 0) {
                log.warn("[压缩] 整轮压缩预算(≤{}ms)已耗尽，停止 failover、走硬截断降级（避免拖死用户那一轮）",
                        overallDeadline - compLoopStart);
                break;
            }
            long compStart = System.currentTimeMillis();
            ForwardEngine.ForwardResult r = forwardEngine.execute(new LinkedHashMap<>(compressReq), s.candidate,
                    false, Long.MAX_VALUE, budget);
            long compMs = System.currentTimeMillis() - compStart;
            if (r.kind == ForwardEngine.ForwardResult.Kind.SUCCESS) {
                List<?> choices = Json.asList(r.parsedBody.get("choices"));
                Map<String, Object> first = choices == null || choices.isEmpty() ? null : Json.asMap(choices.get(0));
                Map<String, Object> m = first == null ? null : Json.asMap(first.get("message"));
                if (m != null && m.get("content") instanceof String s2 && !s2.isBlank()) {
                    // 质量门槛：>5K 输入却输出 <50 字符 = 明显没干活（敷衍/空壳），换下一家。
                    // 阈值从 200 下调到 50：精炼的好摘要可能只有百余字，200 会误杀，50 只拦真敷衍。
                    if (inputText.length() > 5000 && s2.length() < 50) {
                        log.warn("压缩候选 {}/{} 输出过短({} 字符)疑似无效，换下一家",
                                s.candidate.providerId(), s.candidate.model(), s2.length());
                        // 软失败：上游正常返回但摘要无效——只记账不惩罚（模型没犯错，不喂熔断）
                        accountCompress(compressorModel, s, compMs, r, idx, "FAILED", "compress output too short", recordClient);
                        continue;
                    }
                    summary = s2;
                    usedTier = tier;
                    accountCompress(compressorModel, s, compMs, r, idx, "OK", null, recordClient);
                }
                if (summary != null) break;
            } else {
                log.warn("压缩候选 {}/{} 失败: {}", s.candidate.providerId(), s.candidate.model(),
                        r.error == null ? "" : r.error);
                // 硬失败：与主路由同套治理（冷却/熔断/限流学习都继承），真实消耗配额的失败必须记账
                try {
                    policyService.recordFailure(s.candidate.providerId(), s.candidate.model(),
                            s.candidate.channelId(), r.status, r.error, recordClient);
                } catch (Exception ignore) {
                }
                accountCompress(compressorModel, s, compMs, r, idx, "FAILED", r.error, recordClient);
            }
        }
        if (summary == null) log.warn("[压缩] 压缩候选链穷尽仍无有效输出，原样转发（免费候选未花钱）");
        else log.info("[压缩] 压缩由 {}/{} 承担（费用档 {}）", "组", compressorModel, usedTier);
        return summary;
    }

    /**
     * 压缩候选链的每轮超时纯决策（可单测、无副作用）：给定整轮墙钟截止点与单条上限，返回本轮该发的
     * HTTP 超时(ms)。剩余时间不足 floorMs（再发一次也可能白等）时返回 0，调用方据此停止 failover 走降级。
     * 绝不返回负数；单条恒 ≤ perCallCapMs（把挂死的免费家从 120s 腰斩到上限），整轮恒 ≤ 墙钟。
     */
    static int nextCompressBudgetMs(long overallDeadlineMs, long perCallCapMs, long nowMs, int floorMs) {
        long remaining = overallDeadlineMs - nowMs;
        if (remaining < floorMs) return 0;
        return (int) Math.min(perCallCapMs, remaining);
    }

    /** 压缩调用记账标识：request_log/usage 里把压缩子请求与主请求区分开（用量看板可按 client 过滤）。 */
    private static final String COMPRESS_CLIENT = "gw-compress";

    /**
     * 压缩子调用落账：token 消耗真实计入该候选的配额（边界：压缩花钱是预期内的），
     * 成功喂 usage 学习。trace 自动继承父请求（同线程 RequestTrace），故链路里能看到这次压缩。
     * 记账异常绝不冒泡打断压缩/请求。
     */
    private void accountCompress(String compressorModel, PolicyService.Scored s, long totalMs,
                                 ForwardEngine.ForwardResult r, int attempts, String status, String error,
                                 String recordClient) {
        try {
            Map<String, Object> usage = r.parsedBody == null ? null : Json.asMap(r.parsedBody.get("usage"));
            Long in = usage == null ? null : Json.longVal(usage.get("prompt_tokens"));
            Long out = usage == null ? null : Json.longVal(usage.get("completion_tokens"));
            if ("OK".equals(status)) {
                // ttft 传 null：非流式压缩无有效首字样本，喂进速度 EWMA 会失真；usage/配额照常累计
                policyService.recordSuccess(s.candidate.providerId(), s.candidate.model(), s.candidate.channelId(),
                        null, in, out, false, recordClient, totalMs);
            }
            statsService.logRequest(compressorModel, s.candidate.providerId(), s.candidate.model(),
                    null, (int) totalMs, in, out, status, attempts, null, error, recordClient,
                    null, null, status.equals("OK") ? 200 : (r.status > 0 ? r.status : null), null,
                    r.channelId != null ? r.channelId : s.candidate.channelId());
        } catch (Exception e) {
            log.debug("[压缩] 记账失败（忽略）: {}", e.getMessage());
        }
    }

    /**
     * #selfcheck 压缩链路一键干跑：用固定 ~5K 字符模拟对话（user/assistant/tool 混合）走一遍
     * 真实压缩候选链（{@link #callCompressor}），记 client=selfcheck（隔离：不进 EWMA/预算/限流学习、
     * 失败不进主熔断），绕开滚动缓存（callCompressor 本就不读写 rolling）。报告候选链是否可用、
     * 产出摘要字数、耗时。目的：让"2a 事故"（候选链全超时→静默退硬截断、日志全绿）当场暴露。
     * 复用真实链逻辑，不改动主路径。
     */
    public Map<String, Object> selfCheckCompress() {
        Map<String, Object> rep = new LinkedHashMap<>();
        GatewayConfig.Defaults d = configService.snapshot().config.getDefaults();
        String model = d.getContextCompressModel();
        if (model == null || model.isBlank()) {
            rep.put("ok", false);
            rep.put("skipped", true);
            rep.put("note", "未配置 contextCompressModel：压缩功能关闭，干跑跳过");
            return rep;
        }
        boolean freeOnly = !model.contains("/");
        String sample = selfCheckSample();
        long t0 = System.currentTimeMillis();
        String summary = callCompressor(COMPRESS_PROMPT, sample, model, d, freeOnly, null, "selfcheck");
        rep.put("compressorModel", model);
        rep.put("path", "full（干跑，绕过滚动缓存）");
        rep.put("costMs", System.currentTimeMillis() - t0);
        rep.put("ok", summary != null);
        rep.put("summaryChars", summary == null ? 0 : summary.length());
        if (summary == null) {
            rep.put("note", "压缩候选链全部不可用/失败——真实大上下文将退化到硬截断（无摘要），"
                    + "请检查 contextCompressModel 配置与候选健康/冷却");
        }
        return rep;
    }

    /** 固定干跑样本（~5K 字符，user/assistant/tool 混合）：内容固定 → 结果可复现、可跨次对比。 */
    private static String selfCheckSample() {
        String[] turns = {
                "user: 我要把一个 Spring Boot 网关接入多个免费上游模型，重点解决免费模型爱提前停止的问题。",
                "assistant: 先理清早停的三类诱因：上下文过长被压缩后只剩总结口吻、tool 定义缺失、max_tokens 太小。",
                "tool: {\"read_file\":\"src/main/java/GatewayController.java\",\"bytes\":5120}",
                "assistant: 读到的转发主循环里，STREAM 分支在拿到首字后就 releaseInflight，排空阶段没占并发位。",
                "user: 那并发达上限时为什么日志还是全绿？",
                "tool: {\"grep\":\"concFull\",\"hits\":10}",
                "assistant: 命中 10 次都在流式排空期，说明并发额度被提前释放，真实长流互相挤爆免费上游的 concurrency limit。",
                "user: 压缩链路最近是不是也出过静默降级？",
                "assistant: 是的，2a 的响应头超时误伤非流式，压缩候选链全部超时后退到 hard-truncate，请求照样 200，日志看不出。",
        };
        StringBuilder sb = new StringBuilder();
        for (int round = 0; round < 16; round++) for (String t : turns) sb.append(t).append('\n');
        return sb.toString();
    }

    /** messages[0..end) 的内容哈希（append-only 校验：前缀一致=同一会话续写） */
    private static String hashOf(List<Object> messages, int end) {
        java.security.MessageDigest md;
        try {
            md = java.security.MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            return null;
        }
        // #12 尾锚点增量：只哈希截点前最后 HASH_ANCHOR_WINDOW 条 + 截点条数，避免每次整段 O(n) 序列化。
        // 会话指纹已进桶 key（rollingKey），跨会话不再共享状态；此哈希只需捕捉"同会话内近期历史
        // 被编辑/rewind"——那必然改动尾锚点窗口内容或使条数倒退（条数已单独入摘要种子）。
        md.update(("count=" + end + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (int i = Math.max(0, end - HASH_ANCHOR_WINDOW); i < end; i++) {
            Map<String, Object> m = Json.asMap(messages.get(i));
            if (m == null) continue;
            String role = Json.str(m.get("role"));
            Object c = m.get("content");
            byte[] part = (role + "|" + (c == null ? "" : Json.write(c)) + "|"
                    + (m.get("tool_calls") == null ? "" : Json.write(m.get("tool_calls"))) + "\n")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            md.update(part);
        }
        byte[] dig = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : dig) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /**
     * 滚动缓存分桶 key = client + 会话指纹。
     * 边界 5：只按 system 哈希不够（Claude Code 子代理/多窗口/rewind 的 system 完全相同），
     * 指纹取「首条 system 内容 + 前 2 条 user 内容」组合哈希——会话内稳定、跨会话可分。
     */
    private static String rollingKey(List<Object> messages, String client) {
        String cl = client == null || client.isBlank() ? "unknown" : client;
        return cl + "#" + sessionFingerprint(messages);
    }

    private static String sessionFingerprint(List<Object> messages) {
        java.security.MessageDigest md;
        try {
            md = java.security.MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            return "na";
        }
        StringBuilder seed = new StringBuilder();
        boolean gotSystem = false;
        int users = 0;
        for (Object o : messages) {
            Map<String, Object> m = Json.asMap(o);
            if (m == null) continue;
            String role = Json.str(m.get("role"));
            Object c = m.get("content");
            String text = c == null ? "" : (c instanceof String s ? s : Json.write(c));
            if ("system".equals(role)) {
                if (!gotSystem) { gotSystem = true; seed.append("S:").append(text).append('\n'); }
            } else if ("user".equals(role)) {
                if (users < 2) { users++; seed.append("U").append(users).append(':').append(text).append('\n'); }
            }
            if (gotSystem && users >= 2) break;
        }
        if (seed.length() == 0) return "na";   // 无 system 无前 2 条 user：退化到 client 单键
        md.update(seed.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        byte[] dig = md.digest();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8 && i < dig.length; i++) sb.append(String.format("%02x", dig[i]));
        return sb.toString();
    }

    /** 滚动缓存容量兜底：超限时删最旧（单机工具数量有限，正常不触发） */
    private void evictOldest() {
        String oldest = null; long oldestAt = Long.MAX_VALUE;
        for (Map.Entry<String, RollingState> e : rolling.entrySet()) {
            if (e.getValue().lastUsedAt < oldestAt) { oldestAt = e.getValue().lastUsedAt; oldest = e.getKey(); }
        }
        if (oldest != null) rolling.remove(oldest);
    }

    /** 开头连续 system 消息的结束下标（0~k） */
    private static int firstNonSystem(List<Object> messages) {
        int i = 0;
        while (i < messages.size()) {
            Map<String, Object> m = Json.asMap(messages.get(i));
            if (m == null || !"system".equals(Json.str(m.get("role")))) break;
            i++;
        }
        return i;
    }

    /**
     * 压缩/硬截断后，若保留段 [keepFrom..n) 不含任何 user 消息，取回"最近一条 user 指令"补回（只留
     * role+content，不携带被覆盖区里的 tool_calls，避免造出孤儿 tool → 上游 400）。保留段已有 user、
     * 或全表都没有 user → 返回 null（不补）。
     *
     * 动机：Agent 工具循环里 user 消息稀疏（一条指令后接几十上百条 assistant/tool）。keepRecentMessages
     * 条的保留段往往整段都是工具循环尾，会把"用户当前要干嘛"这条指令连同更早历史一起压进摘要段，结果
     * 压缩后整份列表没有任何 user-role 消息——agnes 报 "No user query found in messages"、zhipu 报
     * 1214 "messages 参数非法"（都要求至少一条 user 消息）；且 flash 小模型拿不到活动指令可遵循，
     * 读到摘要里"已完成的关键步骤"就顺势收尾 → 静默早停/糊弄。补回这条指令既满足上游校验，也告诉模型
     * "接着干什么"。
     */
    private static Map<String, Object> userInstructionIfMissing(List<Object> messages, int keepFrom, int n) {
        // 1) 保留段内已有非空 user 消息 → 无需补
        for (int i = keepFrom; i < n; i++) {
            Map<String, Object> m = Json.asMap(messages.get(i));
            if (m != null && "user".equals(Json.str(m.get("role"))) && hasContent(m)) return null;
        }
        // 2) 从保留段之前往前找最近一条非空 user 消息（必然落在被摘要/截断的覆盖区）
        for (int i = Math.max(keepFrom, 1) - 1; i >= 0; i--) {
            Map<String, Object> um = Json.asMap(messages.get(i));
            if (um != null && "user".equals(Json.str(um.get("role"))) && hasContent(um)) {
                Map<String, Object> clean = new LinkedHashMap<>();
                clean.put("role", "user");
                clean.put("content", um.get("content"));
                return clean;
            }
        }
        return null;
    }

    /** user 消息是否有实质内容（content 为 null/空白/空壳结构视为无） */
    private static boolean hasContent(Map<String, Object> m) {
        Object c = m.get("content");
        if (c == null) return false;
        if (c instanceof String s) return !s.isBlank();
        if (c instanceof List<?> l) return !l.isEmpty();
        return true;
    }

    /** 把保留边界对齐到工具组边界：assistant(tool_calls) 与其后续 tool 消息不可拆开，
     *  否则保留段开头出现孤儿 tool（上游 400）、或历史段里有 tool_calls 而无结果。
     *  统一方向 = 向前拉 owner 并入保留段（工具组原文完整保留是安全方向）：
     *  旧实现的 case 1 是"向后吞结果串"，当末尾并行工具串横跨切点时会把
     *  "刚刚跑完的工具输出"整组吞进摘要、保留段甚至清空——那正是静默早停最需要的现场。
     *  拉进来的超大输出由 shrinkOversizedText（头70%+尾30%）消化，不撑爆保留预算。 */
    private static int alignToToolGroupBoundary(List<Object> messages, int keepFrom, int sysEnd) {
        if (keepFrom <= 0 || keepFrom >= messages.size()) return keepFrom;
        // 1) keepFrom-1 是带 tool_calls 的 assistant（切点落在其结果串中间）→ 拉 owner 进保留段
        Map<String, Object> prev = Json.asMap(messages.get(keepFrom - 1));
        if (keepFrom - 1 > sysEnd && prev != null
                && "assistant".equals(Json.str(prev.get("role"))) && prev.get("tool_calls") != null) {
            keepFrom--;
        }
        // 2) 保留段开头是 tool 串（其 assistant 更早落在历史段）→ 向前扩：
        //    越过整串 tool 并把所属 assistant(tool_calls) 一并纳入保留段。
        //    （旧实现只越过 tool 串、停在 assistant 之后，保留段依然孤儿 tool → 上游 400）
        while (keepFrom > sysEnd + 1 && keepFrom < messages.size()) {
            Map<String, Object> at = Json.asMap(messages.get(keepFrom));
            if (at == null || !"tool".equals(Json.str(at.get("role")))) break;
            Map<String, Object> p = Json.asMap(messages.get(keepFrom - 1));
            if (p == null) break;
            String pr = Json.str(p.get("role"));
            if ("tool".equals(pr)) { keepFrom--; continue; }
            if ("assistant".equals(pr) && p.get("tool_calls") != null) { keepFrom--; }
            break;   // 非 tool 串也非 owner：再往前扩无意义
        }
        return keepFrom;
    }

    /** 消息转可读文本行（历史段喂压缩模型的格式） */
    private static String roleLine(Map<String, Object> m) {
        String role = Json.str(m.get("role"));
        Object c = m.get("content");
        String text;
        if (c instanceof String s) text = s;
        else if (c instanceof List<?> parts) {
            StringBuilder sb = new StringBuilder();
            for (Object o : parts) {
                Map<String, Object> p = Json.asMap(o);
                if (p != null && p.get("text") instanceof String s) sb.append(s).append(' ');
                else if (p != null && p.get("image_url") != null) sb.append("[图片] ");
            }
            text = sb.toString().strip();
        } else text = "";
        // tool_calls：函数名 + 参数（参数可能很大，截断保摘要可读）
        Object tc = m.get("tool_calls");
        List<Object> calls = Json.asList(tc);
        StringBuilder cb = new StringBuilder();
        if (calls != null) {
            for (Object call : calls) {
                Map<String, Object> cm = Json.asMap(call);
                if (cm == null) continue;
                Map<String, Object> fn = Json.asMap(cm.get("function"));
                if (fn == null) continue;
                String args = fn.get("arguments") instanceof String s ? s : "";
                if (args.length() > 500) args = args.substring(0, 500) + "…(截断)";
                cb.append(" [调用工具 ").append(fn.get("name")).append("(").append(args).append(")]");
            }
        }
        // 超长单条内容截断（防一条巨长日志吃掉全部压缩预算）
        if (text.length() > 4000) text = text.substring(0, 4000) + "…(内容过长截断)";
        if (text.isEmpty() && cb.length() == 0) return "";
        return "[" + role + "] " + text + cb;
    }
}
