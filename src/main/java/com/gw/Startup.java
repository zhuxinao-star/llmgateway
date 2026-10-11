package com.gw;

import com.gw.catalog.CatalogService;
import com.gw.config.ConfigService;
import com.gw.policy.PolicyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
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
                                 PolicyService policyService) {
        return args -> {
            configService.init();
            policyService.restoreRuntime();
            catalogService.start();
            log.info("LLM Gateway 启动完成: http://{}:{}  (管理页 /ui)",
                    configService.snapshot().config.getServer().getBind(),
                    configService.snapshot().config.getServer().getPort());
        };
    }
}
