package com.github.monkeywie.proxyee.handler;

import com.github.monkeywie.proxyee.server.HttpProxyServerConfig;
import com.github.monkeywie.proxyee.util.ProtoUtil.RequestProto;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.proxy.ProxyHandler;

/**
 * HTTP代理，转发解码后的HTTP报文
 */
public class HttpProxyInitializer extends ChannelInitializer {

    private final Channel clientChannel;
    private final RequestProto requestProto;
    private final ProxyHandler proxyHandler;
    private final WebsocketProxyHandler wsHandler;

    public HttpProxyInitializer(Channel clientChannel, RequestProto requestProto,
                                ProxyHandler proxyHandler, WebsocketProxyHandler wsHandler) {
        this.clientChannel = clientChannel;
        this.requestProto = requestProto;
        this.proxyHandler = proxyHandler;
        this.wsHandler = wsHandler;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        if (proxyHandler != null) {
            ch.pipeline().addLast(proxyHandler);
        }
        HttpProxyServerConfig serverConfig = ((HttpProxyServerHandler) clientChannel.pipeline().get("serverHandle")).getServerConfig();
        if (requestProto.getSsl()) {
            ch.pipeline().addLast(serverConfig.getClientSslCtx().newHandler(ch.alloc(), requestProto.getHost(), requestProto.getPort()));
        }
        ch.pipeline().addLast("httpCodec", new HttpClientCodec(
                serverConfig.getMaxInitialLineLength(),
                serverConfig.getMaxHeaderSize(),
                serverConfig.getMaxChunkSize()));
        if (this.wsHandler != null) {
            ch.pipeline().addLast("decompress", new HttpContentDecompressor());
            ch.pipeline().addLast("aggregator", new HttpObjectAggregator(1024 * 1024 * 8));
            ch.pipeline().addLast("wsCompression", WebsocketCompressionHandler.INSTANCE);
            ch.pipeline().addLast("wsAggregator", new WebSocketFrameAggregator(serverConfig.getWsDecoderConfig().maxFramePayloadLength()));
            ch.pipeline().addLast("wsHandler", this.wsHandler);
        }
        ch.pipeline().addLast("proxyClientHandle", new HttpProxyClientHandler(clientChannel));
    }
}
