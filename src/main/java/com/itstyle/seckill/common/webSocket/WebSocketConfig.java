package com.itstyle.seckill.common.webSocket;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

/**
 * @author ZGY
 * @description 自定义 Websocket 服务端点配置
 */
@Configuration  
public class WebSocketConfig {

    /**
     * 有了这个 Bean，就可以使用＠ServerEndpoint 定义一个端点服务类。
     * @return
     */
    @Bean  
    public ServerEndpointExporter serverEndpointExporter() {  
        return new ServerEndpointExporter();  
    }  
}  
