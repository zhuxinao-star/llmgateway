package com.gw;

import com.gw.policy.UpstreamErrorClassifier.ErrorClass;
import com.gw.probe.ModelReconciler;
import com.gw.probe.ModelReconciler.Action;

/**
 * 快速单测（main 直跑）：ModelReconciler.decide 纯决策回归。
 * 铁律——"上游列表查不到"绝不单独触发停用（保护智谱类"查不到但能调"），
 * 停与恢都以"确认探测"结论为准；非模型死活的失败（限流/超时/鉴权）一律不动手（fail-open）。
 */
public class ModelReconcilerTest {

    static int failures = 0;

    static void check(String name, Object expected, Object actual) {
        boolean ok = expected == null ? actual == null : expected.equals(actual);
        System.out.println((ok ? "PASS " : "FAIL ") + name + (ok ? "" : "  期望=" + expected + " 实际=" + actual));
        if (!ok) failures++;
    }

    static Action decide(boolean ran, boolean ok, ErrorClass cls, boolean inList, boolean autoDis) {
        return ModelReconciler.decide(ran, ok, cls, inList, autoDis);
    }

    public static void main(String[] args) {
        // 启用中的模型（autoDis=false）
        check("没探测过→绝不动手", Action.NONE, decide(false, false, null, false, false));
        check("列表没有但探测成功→不动（保护智谱类）", Action.NONE, decide(true, true, null, false, false));
        check("列表没有+探测报模型不存在→永久停用", Action.PERMANENT_DISABLE,
                decide(true, false, ErrorClass.MODEL_NOT_SUPPORTED, false, false));
        check("列表里有但探测报模型不存在→仍永久停用（调用为准）", Action.PERMANENT_DISABLE,
                decide(true, false, ErrorClass.MODEL_NOT_SUPPORTED, true, false));
        check("探测报余额不足→临时停用", Action.TEMP_DISABLE,
                decide(true, false, ErrorClass.QUOTA, false, false));
        check("探测报限流→不动（非死活证据）", Action.NONE,
                decide(true, false, ErrorClass.RATE_LIMIT, false, false));
        check("探测报超时→不动", Action.NONE,
                decide(true, false, ErrorClass.TIMEOUT, false, false));
        check("探测报鉴权→不动", Action.NONE,
                decide(true, false, ErrorClass.AUTH, false, false));
        check("探测报服务忙→不动", Action.NONE,
                decide(true, false, ErrorClass.SERVER_BUSY, false, false));

        // 被本器自动停用中的模型（autoDis=true）：恢复路径
        check("自动停用+重新进列表→恢复（免探测）", Action.RECOVER, decide(false, false, null, true, true));
        check("自动停用+不在列表+探测成功→恢复", Action.RECOVER, decide(true, true, null, false, true));
        check("自动停用+不在列表+未探测→维持停用", Action.NONE, decide(false, false, null, false, true));
        check("自动停用+不在列表+探测仍失败→维持停用", Action.NONE,
                decide(true, false, ErrorClass.MODEL_NOT_SUPPORTED, false, true));
        // 人工停用（autoDis=false 语义下即使报不存在也只走标准停用；此路径不经 decide 恢复）
        check("不在列表但能调通（恢复态误判防护）", Action.NONE, decide(true, true, null, false, false));

        System.out.println(failures == 0 ? "\nALL PASS" : "\nFAILURES=" + failures);
        if (failures > 0) System.exit(1);
    }
}
