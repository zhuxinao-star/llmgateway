package com.gw.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 管理面最小鉴权（十六点清单 #8 附带，评审提入第 2 批）：
 * /api/** 的写方法（POST/PUT/DELETE/PATCH）在配置了 admin-token 后需凭据；
 * 凭据取 x-admin-token 头或 Authorization: Bearer，值等于 admin-token 或任一 gateway-key 即放行
 * （与 gateway-keys 同源管理：本机日常用现有 gw key 即可，无需记第二个秘密）。
 * token 缺省（正式配置默认态）时整体放行——防自锁：界面/脚本先于配置升级也不会被挡在门外。
 * GET 类只读接口不拦：本机读泄露风险远小于写风险，且 overview/usage 轮询高频。
 */
@Component
public class AdminAuthFilter extends OncePerRequestFilter {

    private final ConfigService configService;

    public AdminAuthFilter(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        String path = req.getRequestURI();
        String m = req.getMethod();
        boolean write = "POST".equals(m) || "PUT".equals(m) || "DELETE".equals(m) || "PATCH".equals(m);
        if (!write || !path.startsWith("/api/")) {
            chain.doFilter(req, resp);
            return;
        }
        ConfigService.Snapshot snap = configService.snapshot();
        if (snap.adminTokenHash == null) {  // 未配置 token = 不启用（防自锁）
            chain.doFilter(req, resp);
            return;
        }
        String cred = req.getHeader("x-admin-token");
        if (cred == null || cred.isBlank()) {
            String auth = req.getHeader("Authorization");
            if (auth != null && auth.startsWith("Bearer ")) cred = auth.substring(7).trim();
        }
        if (cred != null && !cred.isBlank()) {
            String hash = ConfigService.sha256(cred.trim());
            if (hash.equals(snap.adminTokenHash) || snap.gatewayKeyHashes.containsKey(hash)) {
                chain.doFilter(req, resp);
                return;
            }
        }
        resp.setStatus(401);
        resp.setContentType("application/json;charset=UTF-8");
        resp.getOutputStream().write(
                "{\"error\":\"管理面写操作需要凭据：x-admin-token 或 Authorization: Bearer <admin-token/gateway-key>\"}"
                        .getBytes(StandardCharsets.UTF_8));
    }
}
