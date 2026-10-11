package com.gw;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.tomcat.TomcatProtocolHandlerCustomizer;

@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(GatewayApplication.class);
        // Virtual threads for SSE long-lived connections (JDK 21)
        app.setRegisterShutdownHook(true);
        app.run(args);
    }

    @org.springframework.context.annotation.Bean
    public TomcatProtocolHandlerCustomizer<?> protocolHandlerVirtualThreads() {
        return protocolHandler -> protocolHandler.setExecutor(
                java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
    }
}
