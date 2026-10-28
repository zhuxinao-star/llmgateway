package com.gw;

import com.gw.catalog.CatalogService;
import com.gw.config.ConfigService;
import com.gw.policy.PolicyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 启动装配：配置加载 → 运行时恢复 → 目录发现 → 定时任务。
 */
@Configuration
public class Startup {

    private static final Logger log = LoggerFactory.getLogger(Startup.class);

    @Bean
    CommandLineRunner bootstrap(ConfigService configService, CatalogService catalogService,
                                 PolicyService policyService, ServerProperties serverProperties) {
        return args -> {
            configService.init();
            policyService.restoreRuntime();
            policyService.refreshChronicRateLimits();   // 冷启动第一发就认出长期撞墙的家，不等 60s 调度
            catalogService.start();
            // 这行日志说的是进程真正按哪个端口起来了，不是 config.yaml 里写的那个数：
            // 监听端口只由 application.yaml 的 server.port 和 -Dserver.port（GW_PORT）决定，
            // config.yaml 的 server 段以前会顶掉这里，让人改错文件（实测注入 9000 仍报 8317）。
            Integer bound = serverProperties.getPort();
            String host = serverProperties.getAddress() != null
                    ? serverProperties.getAddress().getHostAddress()
                    : configService.snapshot().config.getServer().getBind();
            log.info("LLM Gateway 启动完成: http://{}:{}  (管理页 /ui)",
                    host, bound == null ? configService.snapshot().config.getServer().getPort() : bound);
            int cfgPort = configService.snapshot().config.getServer().getPort();
            if (bound != null && bound != cfgPort) {
                log.warn("config.yaml 的 server.port={} 与实际监听端口 {} 不一致：这一项不影响监听，"
                        + "要改端口请用 src/main/resources/application.yaml 或环境变量 GW_PORT", cfgPort, bound);
            }
        };
    }
}
