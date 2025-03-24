package com.github.monkeywie.proxyee.intercept;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

/**
 * http拦截器
 * beforeConnect -> beforeRequest -> afterResponse
 */
public class HttpProxyIntercept {

    /**
     * 在与目标服务器建立连接之前拦截
     */
    public void beforeConnect(Channel clientChannel, HttpProxyInterceptPipeline pipeline) throws Exception {
        pipeline.beforeConnect(clientChannel);
    }

    /**
     * 拦截代理服务器到目标服务器的请求头
     */
    public void beforeRequest(Channel clientChannel, HttpRequest httpRequest,
                              HttpProxyInterceptPipeline pipeline) throws Exception {
        pipeline.beforeRequest(clientChannel, httpRequest);
    }

    /**
     * 拦截代理服务器到目标服务器的请求体
     */
    public void beforeRequest(Channel clientChannel, HttpContent httpContent,
                              HttpProxyInterceptPipeline pipeline) throws Exception {
        pipeline.beforeRequest(clientChannel, httpContent);
    }

    /**
     * 拦截代理服务器到客户端的响应头
     */
    public void afterResponse(Channel clientChannel, Channel proxyChannel, HttpResponse httpResponse,
                              HttpProxyInterceptPipeline pipeline) throws Exception {
        pipeline.afterResponse(clientChannel, proxyChannel, httpResponse);
    }


    /**
     * 拦截代理服务器到客户端的响应体
     */
    public void afterResponse(Channel clientChannel, Channel proxyChannel, HttpContent httpContent,
                              HttpProxyInterceptPipeline pipeline)
            throws Exception {
        pipeline.afterResponse(clientChannel, proxyChannel, httpContent);
    }

    public void onWebsocketHandshakeCompleted(HttpProxyInterceptPipeline pipeline) {
        pipeline.websocketHandshakeCompleted();
    }

    public void onWebsocketRequest(Channel clientChannel,
                                   Channel proxyChannel,
                                   WebSocketFrame webSocketFrame,
                                   HttpProxyInterceptPipeline pipeline) throws Exception {
        pipeline.websocketRequest(clientChannel, proxyChannel, webSocketFrame);
    }

    public void onWebsocketResponse(Channel clientChannel,
                                    Channel proxyChannel,
                                    WebSocketFrame webSocketFrame,
                                    HttpProxyInterceptPipeline pipeline) throws Exception {
        pipeline.websocketResponse(clientChannel, proxyChannel, webSocketFrame);
    }

    public void onWebsocketClose(HttpProxyInterceptPipeline pipeline) {
        pipeline.websocketClose();
    }
}
