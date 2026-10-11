package com.gw.util;

/**
 * 客户端识别：把请求 User-Agent 归一化成简短工具名，
 * 用于请求日志与按工具用量统计。匹配不到时截取 UA 前 24 字符。
 */
public final class ClientDetector {

    private ClientDetector() {}

    public static String detect(String ua) {
        if (ua == null || ua.isBlank()) return "unknown";
        String s = ua.toLowerCase();
        if (s.contains("zcode")) return "zcode";
        if (s.contains("claude-cli") || s.contains("claude-code") || s.contains("claudecode")) return "claude-code";
        if (s.contains("cline")) return "cline";
        if (s.contains("roo-code") || s.contains("roocode")) return "roo-code";
        if (s.contains("cherry")) return "cherry-studio";
        if (s.contains("chatbox")) return "chatbox";
        if (s.contains("nextchat")) return "nextchat";
        if (s.contains("lobechat")) return "lobechat";
        if (s.contains("openwebui") || s.contains("open-webui")) return "open-webui";
        if (s.contains("cursor")) return "cursor";
        if (s.contains("windsurf")) return "windsurf";
        if (s.contains("vscode") || s.contains("visual studio code")) return "vscode";
        if (s.contains("continue/")) return "continue";
        if (s.contains("openai-python")) return "openai-python";
        if (s.contains("openai-node") || s.contains("openai-js")) return "openai-node";
        if (s.contains("anthropic-sdk") || s.contains("anthropic-python")) return "anthropic-sdk";
        if (s.contains("httpx") || s.contains("python")) return "python";
        if (s.contains("node") || s.contains("axios")) return "node";
        if (s.contains("go-http")) return "go";
        if (s.contains("java")) return "java";
        if (s.contains("curl")) return "curl";
        if (s.contains("mozilla") || s.contains("chrome") || s.contains("edg/") || s.contains("safari"))
            return "browser";
        return s.length() > 24 ? s.substring(0, 24) : s;
    }
}
