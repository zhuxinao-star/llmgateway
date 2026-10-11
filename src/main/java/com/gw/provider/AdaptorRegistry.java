package com.gw.provider;

import com.gw.config.ConfigService;
import com.gw.config.GatewayConfig;
import com.gw.provider.adaptor.AnthropicAdaptor;
import com.gw.provider.adaptor.OpenAiAdaptor;
import com.gw.provider.adaptor.ProtocolAdaptor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/** 适配器注册表：protocol → Adaptor 工厂（适配器有流式状态，按请求创建实例） */
@Service
public class AdaptorRegistry {

    private final Map<String, ProtocolAdaptor> protos = new HashMap<>();

    public AdaptorRegistry() {
        OpenAiAdaptor openAi = new OpenAiAdaptor();
        protos.put(openAi.protocol(), openAi);
        AnthropicAdaptor anthropic = new AnthropicAdaptor();
        protos.put(anthropic.protocol(), anthropic);
    }

    /** 按请求创建新实例（流式转换有状态） */
    public ProtocolAdaptor create(String protocol) {
        ProtocolAdaptor proto = protos.get(protocol);
        if (proto == null) throw new IllegalArgumentException("不支持的协议: " + protocol);
        if (proto instanceof OpenAiAdaptor) return new OpenAiAdaptor();
        if (proto instanceof AnthropicAdaptor) return new AnthropicAdaptor();
        return proto;
    }

    public boolean supports(String protocol) { return protos.containsKey(protocol); }
}
