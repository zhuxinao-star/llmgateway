package com.gw.forward;

import com.gw.provider.adaptor.Json;

import java.lang.reflect.Method;
import java.util.*;

/**
 * 快速单测（main 运行）：验证 v8.6 压缩修复——压缩保留段若全是工具循环（无 user 消息），
 * 必须补回"最近一条 user 指令"，否则 agnes 报 "No user query found in messages"、zhipu 报 1214，
 * 且 flash 小模型拿不到活动指令 → 静默早停。
 *
 * 直接反射调用 ContextCompressor.userInstructionIfMissing（上线逻辑），三用例：
 *  1. DSH 场景：user 指令在保留段之前（被摘要覆盖）→ 返回该 user 消息；
 *  2. 保留段内已有 user → 返回 null（不重复插入）；
 *  3. 全表无 user → 返回 null。
 */
public class ContextCompressRetainUserTest {

    public static void main(String[] args) throws Exception {
        Method m = ContextCompressor.class.getDeclaredMethod(
                "userInstructionIfMissing", List.class, int.class, int.class);
        m.setAccessible(true);

        boolean ok = true;

        // ---- 用例 1：DSH 工具循环——user 指令在保留段之前 ----
        // messages: [system, user("继续，为什么老是断开"), 30×(assistant(tool_calls)+tool)]
        List<Object> msg = new ArrayList<>();
        msg.add(Map.of("role", "system", "content", "工具系统提示"));
        msg.add(Map.of("role", "user", "content", "继续，为什么老是断开，还有项目没用到的表就不要去分析了"));
        for (int i = 0; i < 30; i++) {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("role", "assistant");
            a.put("content", "");
            a.put("tool_calls", List.of(Map.of("id", "call_" + i, "type", "function",
                    "function", Map.of("name", "read", "arguments", "{\"path\":\"f" + i + ".java\"}"))));
            msg.add(a);
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("role", "tool");
            t.put("tool_call_id", "call_" + i);
            t.put("content", "file " + i + " body");
            msg.add(t);
        }
        int n = msg.size();                       // 62
        int keepFrom = n - 15;                    // 保留最近 15 条 = 全工具循环尾，无 user
        Map<String, Object> r1 = (Map<String, Object>) m.invoke(null, msg, keepFrom, n);
        System.out.printf("用例1 保留段[keepFrom=%d..%d) 无 user → 补回：%s%n",
                keepFrom, n, r1 == null ? "null(失败!)" : ("role=" + r1.get("role") + " content=" + Json.str(r1.get("content"))));
        boolean c1 = r1 != null && "user".equals(r1.get("role"))
                && "继续，为什么老是断开，还有项目没用到的表就不要去分析了".equals(Json.str(r1.get("content")));
        ok &= c1;
        System.out.println("  断言1(补回 user 指令): " + (c1 ? "PASS" : "FAIL"));

        // ---- 用例 2：保留段内已有 user → 不插入 ----
        List<Object> msg2 = new ArrayList<>(msg);
        msg2.set(n - 1, Map.of("role", "user", "content", "最后一步继续"));   // 保留段末尾是 user
        int keepFrom2 = n - 15;
        Map<String, Object> r2 = (Map<String, Object>) m.invoke(null, msg2, keepFrom2, n);
        System.out.println("用例2 保留段内已有 user → 返回：" + (r2 == null ? "null(正确)" : "误插入(失败!)"));
        boolean c2 = r2 == null;
        System.out.println("  断言2(不重复插入): " + (c2 ? "PASS" : "FAIL"));
        ok &= c2;

        // ---- 用例 3：全表无 user → 不插入 ----
        List<Object> msg3 = new ArrayList<>();
        msg3.add(Map.of("role", "system", "content", "系统"));
        for (int i = 0; i < 20; i++) {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("role", "assistant");
            a.put("content", "");
            a.put("tool_calls", List.of());
            msg3.add(a);
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("role", "tool");
            t.put("tool_call_id", "x");
            t.put("content", "r");
            msg3.add(t);
        }
        int n3 = msg3.size();
        Map<String, Object> r3 = (Map<String, Object>) m.invoke(null, msg3, n3 - 15, n3);
        System.out.println("用例3 全表无 user → 返回：" + (r3 == null ? "null(正确)" : "误插入(失败!)"));
        boolean c3 = r3 == null;
        System.out.println("  断言3(无 user 不插): " + (c3 ? "PASS" : "FAIL"));
        ok &= c3;

        // ---- 用例 4-7：alignToToolGroupBoundary 边界方向翻转（1b 修复）----
        // 旧实现 case1 把切点"向后吞"过工具结果串——末尾并行工具串横跨切点时，
        // 刚跑完的工具输出被整组吞进摘要、保留段清空（静默早停最需要的现场）。
        // 新实现统一"向前拉 owner 进保留段"。
        Method al = ContextCompressor.class.getDeclaredMethod(
                "alignToToolGroupBoundary", List.class, int.class, int.class);
        al.setAccessible(true);

        // 构造: [system, user, assistant(3路并行tool_calls)@2, tool@3, tool@4, tool@5, assistant(纯文本)@6, user@7]
        List<Object> msg4 = new ArrayList<>();
        msg4.add(Map.of("role", "system", "content", "s"));
        msg4.add(Map.of("role", "user", "content", "跑个并行工具"));
        Map<String, Object> owner = new LinkedHashMap<>();
        owner.put("role", "assistant");
        owner.put("content", "");
        owner.put("tool_calls", List.of(
                Map.of("id", "c1", "type", "function", "function", Map.of("name", "grep", "arguments", "{}")),
                Map.of("id", "c2", "type", "function", "function", Map.of("name", "read", "arguments", "{}")),
                Map.of("id", "c3", "type", "function", "function", Map.of("name", "ls", "arguments", "{}"))));
        msg4.add(owner);
        for (String id : new String[]{"c1", "c2", "c3"})
            msg4.add(Map.of("role", "tool", "tool_call_id", id, "content", "result " + id));
        msg4.add(Map.of("role", "assistant", "content", "三个工具结果如下…"));
        msg4.add(Map.of("role", "user", "content", "继续分析"));
        int sysEnd4 = 1;

        // 用例4：切点落在并行工具串中间（@4）→ 拉 owner 进保留段（=2），保留段绝不空
        int k4 = (int) al.invoke(null, msg4, 4, sysEnd4);
        boolean c4 = k4 == 2 && k4 < msg4.size();
        System.out.printf("%n用例4 切点@工具串中间(4) → 对齐到 %d（期望2=owner，旧实现会吞到8清空保留段）: %s%n", k4, c4 ? "PASS" : "FAIL");
        ok &= c4;

        // 用例5：切点恰好是首个 tool（@3）→ case1 拉 owner → 2
        int k5 = (int) al.invoke(null, msg4, 3, sysEnd4);
        boolean c5 = k5 == 2;
        System.out.printf("用例5 切点@首个tool(3) → 对齐到 %d（期望2）: %s%n", k5, c5 ? "PASS" : "FAIL");
        ok &= c5;

        // 用例6：切点落在普通 user（@7）→ 不动
        int k6 = (int) al.invoke(null, msg4, 7, sysEnd4);
        boolean c6 = k6 == 7;
        System.out.printf("用例6 切点@普通user(7) → 保持 %d（期望7，不过度扩界）: %s%n", k6, c6 ? "PASS" : "FAIL");
        ok &= c6;

        // 用例7：owner 紧贴 system（无历史段可让）→ 下限守卫，不拉穿 sysEnd+1
        List<Object> msg7 = new ArrayList<>();
        msg7.add(Map.of("role", "system", "content", "s"));
        Map<String, Object> owner7 = new LinkedHashMap<>(owner);
        msg7.add(owner7);                                        // @1
        msg7.add(Map.of("role", "tool", "tool_call_id", "c1", "content", "r")); // @2
        int k7 = (int) al.invoke(null, msg7, 2, 1);
        // case1 守卫 keepFrom-1(1)>sysEnd(1) 不成立→跳过；case2 循环 keepFrom(2)>sysEnd+1(2) 不成立→不进。原样返回 2。
        boolean c7 = k7 == 2;
        System.out.printf("用例7 tool 串紧贴 system(切点2) → 返回 %d（守卫下限，不拉穿系统段）: %s%n", k7, c7 ? "PASS" : "FAIL");
        ok &= c7;

        // ---- 演示用例1 压缩后最终消息结构（doCompress 重建 3 行的等价展示）----
        int sysEnd = 1;
        List<Object> rebuilt = new ArrayList<>(msg.subList(0, sysEnd));
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("role", "system");
        summary.put("content", "【压缩摘要】...");
        rebuilt.add(summary);
        if (r1 != null) rebuilt.add(r1);
        rebuilt.addAll(msg.subList(keepFrom, n));
        int userCount = 0;
        for (Object o : rebuilt) if ("user".equals(Json.str(Json.asMap(o).get("role")))) userCount++;
        System.out.println("\n演示：压缩后 messages 共 " + rebuilt.size() + " 条，其中 user 消息 " + userCount
                + " 条（修复前为 0 → agnes/zhipu 400）。末条 role=" + Json.str(Json.asMap(rebuilt.get(rebuilt.size() - 1)).get("role")) + "）");
        ok &= userCount == 1;

        System.out.println("\n=== " + (ok ? "ALL PASS ✅" : "HAS FAILURE ❌") + " ===");
        if (!ok) System.exit(1);
    }
}
