package com.gw.util;

import com.gw.config.GatewayConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 出站 HttpClient 的唯一来源：provider.proxy 非空时该供应商的流量走该代理。
 * 转发、模型清单、余额三条路都必须经此取客户端，否则会出现"转发生效、查清单不生效"这类按路径分裂的行为。
 */
@Component
public class ProviderClients {

    private static final Logger log = LoggerFactory.getLogger(ProviderClients.class);

    /** providerId|proxy|connectTimeoutMs → 客户端（共享连接池） */
    private final ConcurrentHashMap<String, HttpClient> cache = new ConcurrentHashMap<>();

    public HttpClient forProvider(GatewayConfig.Provider p, int connectTimeoutMs) {
        String proxy = p.getProxy() == null ? "" : p.getProxy().trim();
        return cache.computeIfAbsent(p.getId() + "|" + proxy + "|" + connectTimeoutMs, k -> {
            HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(connectTimeoutMs));
            if (!proxy.isEmpty()) {
                try {
                    URI u = URI.create(proxy.startsWith("http") ? proxy : "http://" + proxy);
                    b.proxy(ProxySelector.of(new InetSocketAddress(u.getHost(), u.getPort())));
                    log.info("provider {} 的出站请求走代理 {}:{}", p.getId(), u.getHost(), u.getPort());
                } catch (Exception e) {
                    log.warn("代理配置非法，该供应商仍直连: {}", proxy);
                }
            }
            return b.build();
        });
    }
}
