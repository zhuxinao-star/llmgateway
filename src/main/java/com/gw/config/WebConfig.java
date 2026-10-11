package com.gw.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {
    /**
     * CORS 白名单（原为 allowedOrigins("*")：本机任意网页可跨源驱动 /api 写接口）。
     * 管理 UI 与网关同源（127.0.0.1:8317），本就吃不到 CORS；这里只保留本机回环
     * 与 LAN 访问场景（bind 可为局域网 IP，端口以 server.port 为准）。
     * 服务端 SDK（curl/zcode/Claude Code 等）不受 CORS 约束，无需放行。
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns(
                        "http://127.0.0.1:*", "http://localhost:*",
                        "http://[::1]:*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*");
    }

    /** /ui → /ui/index.html（Spring 静态资源不会自动补 index.html） */
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/ui", "/ui/index.html").setStatusCode(HttpStatus.FOUND);
        registry.addRedirectViewController("/ui/", "/ui/index.html").setStatusCode(HttpStatus.FOUND);
        registry.addRedirectViewController("/", "/ui/index.html").setStatusCode(HttpStatus.FOUND);
    }
}
