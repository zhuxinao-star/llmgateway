package com.gw.catalog;

import java.util.*;

/**
 * 快速单测（main 运行）：账户余额路径抽取的三个纯函数
 * extractJsonPath（点分 + 数组下标）/ toNum（数字或字符串数字）/ fmtAmount。
 * 覆盖 deepseek（balance_infos.0.total_balance 字符串元）与
 * openrouter（data.total_credits - data.total_usage 额度）两家真实响应形状。
 */
public class BalancePathTest {

    static int fails = 0;
    static void check(String name, boolean cond, Object got) {
        System.out.printf("%-52s %s  (got=%s)%n", name, cond ? "PASS" : "FAIL ❌", got);
        if (!cond) fails++;
    }

    public static void main(String[] args) {
        // deepseek: {"is_available":true,"balance_infos":[{"currency":"CNY","total_balance":"103.44",...}]}
        Map<String, Object> dsCur = new LinkedHashMap<>();
        dsCur.put("currency", "CNY");
        dsCur.put("total_balance", "103.44");
        dsCur.put("granted_balance", "0.00");
        Map<String, Object> ds = new LinkedHashMap<>();
        ds.put("is_available", true);
        ds.put("balance_infos", new ArrayList<>(List.of(dsCur)));

        Object t = CatalogService.extractJsonPath(ds, "balance_infos.0.total_balance");
        check("deepseek 数组下标取 total_balance", "103.44".equals(t), t);
        Double amt = CatalogService.toNum(t);
        check("deepseek total_balance 字符串→数字", amt != null && amt == 103.44, amt);
        Object cur = CatalogService.extractJsonPath(ds, "balance_infos.0.currency");
        check("deepseek 币种字段", "CNY".equals(cur), cur);

        // openrouter: {"data":{"total_credits":5,"total_usage":2}}
        Map<String, Object> orData = new LinkedHashMap<>();
        orData.put("total_credits", 5);
        orData.put("total_usage", 2);
        Map<String, Object> or = new LinkedHashMap<>();
        or.put("data", orData);
        Object tc = CatalogService.extractJsonPath(or, "data.total_credits");
        Object tu = CatalogService.extractJsonPath(or, "data.total_usage");
        Double credits = CatalogService.toNum(tc);
        Double usage = CatalogService.toNum(tu);
        check("openrouter total_credits 数字直取", credits != null && credits == 5.0, credits);
        double remaining = credits - usage;   // fetchBalance 里的扣减语义
        check("openrouter 余额=已购-已用=3", remaining == 3.0, remaining);

        // 边界：缺失路径 / 越界 / 非数字 / 空 path
        check("缺失键返回 null", CatalogService.extractJsonPath(ds, "nope.x") == null, "null");
        check("数组越界返回 null", CatalogService.extractJsonPath(ds, "balance_infos.9.total_balance") == null, "null");
        check("对非容器再下钻返回 null", CatalogService.extractJsonPath(ds, "is_available.x") == null, "null");
        check("空 path 返回 null", CatalogService.extractJsonPath(ds, "") == null, "null");
        check("非数字字符串 toNum→null", CatalogService.toNum("abc") == null, "null");
        check("带空格数字串可解析", Objects.equals(CatalogService.toNum(" 8.5 "), 8.5), "8.5");
        check("null toNum→null", CatalogService.toNum(null) == null, "null");

        // fmtAmount：整数不带小数，否则两位
        check("fmtAmount 3.0→\"3\"", "3".equals(CatalogService.fmtAmount(3.0)), CatalogService.fmtAmount(3.0));
        check("fmtAmount 103.44→\"103.44\"", "103.44".equals(CatalogService.fmtAmount(103.44)), CatalogService.fmtAmount(103.44));
        check("fmtAmount 0→\"0\"", "0".equals(CatalogService.fmtAmount(0.0)), CatalogService.fmtAmount(0.0));

        System.out.println("\n=== " + (fails == 0 ? "ALL PASS ✅" : fails + " FAILURE(S) ❌") + " ===");
        if (fails != 0) System.exit(1);
    }
}
