package com.gw.config;

import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.List;
import org.yaml.snakeyaml.Yaml;

/**
 * 一次性验证（非破坏）：config.yaml → GatewayConfig → ConfigWriter.write(temp) → 重新解析 temp，
 * 断言 balance 块经"手写序列化白名单"往返不丢。绝不写回真实 config.yaml。
 */
public class BalanceRoundTripTest {
    public static void main(String[] args) throws Exception {
        Path real = Paths.get("config/config.yaml");
        // 反射拿到 ConfigService 私有静态 newYaml()（含 kebab-case + skipMissing 映射）
        Method m = ConfigService.class.getDeclaredMethod("newYaml");
        m.setAccessible(true);
        Yaml y1 = (Yaml) m.invoke(null);
        GatewayConfig c = y1.loadAs(Files.newInputStream(real), GatewayConfig.class);

        int withBal = 0;
        for (GatewayConfig.Provider p : c.getProviders()) if (p.getBalance() != null) withBal++;
        System.out.println("原始解析：providers=" + c.getProviders().size() + " 带balance=" + withBal);

        Path tmp = Files.createTempFile("gw-balance-rt", ".yaml");
        try {
            ConfigWriter.write(c, tmp);
            String out = Files.readString(tmp);
            boolean writerEmitted = out.contains("balance:") && out.contains("/user/balance")
                    && out.contains("balance_infos.0.total_balance");
            System.out.println("ConfigWriter 输出含 balance 块: " + (writerEmitted ? "PASS" : "FAIL ❌"));

            Yaml y2 = (Yaml) m.invoke(null);
            GatewayConfig c2 = y2.loadAs(Files.newInputStream(tmp), GatewayConfig.class);
            GatewayConfig.BalanceProbe ds = null, or = null;
            for (GatewayConfig.Provider p : c2.getProviders()) {
                if (p.getId().equals("deepseek")) ds = p.getBalance();
                if (p.getId().equals("openrouter")) or = p.getBalance();
            }
            boolean ok = true;
            ok &= expect("deepseek 往返非空", ds != null);
            if (ds != null) {
                ok &= expect("deepseek path=/user/balance", "/user/balance".equals(ds.getPath()));
                ok &= expect("deepseek fields[0]", eq(ds.getFields(), "balance_infos.0.total_balance"));
                ok &= expect("deepseek currency-field", "balance_infos.0.currency".equals(ds.getCurrencyField()));
            }
            ok &= expect("openrouter 往返非空", or != null);
            if (or != null) {
                ok &= expect("openrouter path=/credits", "/credits".equals(or.getPath()));
                ok &= expect("openrouter subtract-fields", eq(or.getSubtractFields(), "data.total_usage"));
                ok &= expect("openrouter unit=credits", "credits".equals(or.getUnit()));
            }
            // 其它未配置的供应商应为 null（未被误造）
            int afterBal = 0;
            for (GatewayConfig.Provider p : c2.getProviders()) if (p.getBalance() != null) afterBal++;
            ok &= expect("往返后带balance数量不变(" + withBal + ")", afterBal == withBal);

            // ---- thinkingMode（新增字段）往返：默认 auto 不落地，非 auto 才写行 ----
            // 基线：未改动对象写出的 yaml 里 thinkingMode 行数（真实 config.yaml 目前应为 0）
            Path tmp2 = Files.createTempFile("gw-thinking-rt", ".yaml");
            try {
                ConfigWriter.write(c, tmp2);
                int baseline = count(Files.readString(tmp2), "thinkingMode:");
                System.out.println("基线 thinkingMode 行数 = " + baseline);

                String targetId = null;
                for (GatewayConfig.Provider p : c.getProviders()) {
                    if ("auto".equals(p.getThinkingMode())) { p.setThinkingMode("disable"); targetId = p.getId(); break; }
                }
                ok &= expect("存在可改的 provider", targetId != null);
                Path tmp3 = Files.createTempFile("gw-thinking-rt3", ".yaml");
                try {
                    ConfigWriter.write(c, tmp3);
                    String out3 = Files.readString(tmp3);
                    int after = count(out3, "thinkingMode:");
                    // 只多一行：说明 auto 不落盘、非 auto 才写，且没有误给其它家也写
                    ok &= expect("thinkingMode 只多写 1 行（auto 不落地）", after == baseline + 1);
                    ok &= expect("写出的是 disable", out3.contains("thinkingMode: disable"));

                    Yaml y4 = (Yaml) m.invoke(null);
                    GatewayConfig c4 = y4.loadAs(Files.newInputStream(tmp3), GatewayConfig.class);
                    int backToAuto = 0, stillAuto = 0;
                    for (GatewayConfig.Provider p : c4.getProviders()) {
                        if (p.getId().equals(targetId)) ok &= expect(targetId + " 重新解析回 disable",
                                "disable".equals(p.getThinkingMode()));
                        else {
                            if ("auto".equals(p.getThinkingMode())) stillAuto++;
                            else backToAuto++;
                        }
                    }
                    // 其余家必须仍是 auto（"auto" 也由 setter 兜底，null/空串不会漏出来）
                    ok &= expect("其余 provider 仍为 auto，无一走形(" + stillAuto + ")", backToAuto == 0);
                } finally {
                    Files.deleteIfExists(tmp3);
                }
                for (GatewayConfig.Provider p : c.getProviders())
                    if (p.getId().equals(targetId)) p.setThinkingMode("auto");
            } finally {
                Files.deleteIfExists(tmp2);
            }

            System.out.println("\n=== " + (writerEmitted && ok ? "ALL PASS ✅" : "HAS FAILURE ❌") + " ===");
            if (!(writerEmitted && ok)) System.exit(1);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
    static boolean eq(List<String> l, String v) { return l != null && l.size() == 1 && v.equals(l.get(0)); }
    static int count(String s, String sub) {
        int n = 0, i = 0;
        while ((i = s.indexOf(sub, i)) >= 0) { n++; i += sub.length(); }
        return n;
    }
    static boolean expect(String name, boolean cond) {
        System.out.printf("  %-40s %s%n", name, cond ? "PASS" : "FAIL ❌");
        return cond;
    }
}
