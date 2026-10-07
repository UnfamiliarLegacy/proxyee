package com.github.monkeywie.proxyee.handler;

import com.github.monkeywie.proxyee.server.HttpProxyServerConfig;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.PendingWriteQueue;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import java.nio.channels.ClosedChannelException;

final class HttpProxyClientProtocolHandler extends ChannelDuplexHandler {

    private static final int MAX_HTTP2_CONTENT_LENGTH = 100 * 1024 * 1024;

    private final SslHandler sslHandler;
    private final boolean allowHttp2;
    private final HttpProxyServerConfig serverConfig;
    private PendingWriteQueue pendingWrites;
    private boolean configured;

    HttpProxyClientProtocolHandler(SslHandler sslHandler, boolean allowHttp2,
                                   HttpProxyServerConfig serverConfig) {
        this.sslHandler = sslHandler;
        this.allowHttp2 = allowHttp2;
        this.serverConfig = serverConfig;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        pendingWrites = new PendingWriteQueue(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (configured) {
            ctx.write(msg, promise);
        } else {
            pendingWrites.add(msg, promise);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof SslHandshakeCompletionEvent)) {
            ctx.fireUserEventTriggered(evt);
            return;
        }

        final SslHandshakeCompletionEvent handshake = (SslHandshakeCompletionEvent) evt;
        if (!handshake.isSuccess()) {
            pendingWrites.removeAndFailAll(handshake.cause());
            ctx.fireUserEventTriggered(evt);
            ctx.close();
            return;
        }

        final String protocol = sslHandler.applicationProtocol();
        if (allowHttp2 && ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            addHttp2Codec(ctx.pipeline(), ctx.name());
        } else if (protocol == null || protocol.isEmpty()
                || ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
            addHttp1Codec(ctx.pipeline(), ctx.name(), serverConfig);
        } else {
            final IllegalStateException cause = new IllegalStateException(
                    "Unsupported upstream application protocol: " + protocol);
            pendingWrites.removeAndFailAll(cause);
            ctx.close();
            return;
        }

        configured = true;
        ctx.fireUserEventTriggered(evt);
        pendingWrites.removeAndWriteAll();
        ctx.pipeline().remove(this);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!configured && pendingWrites != null && !pendingWrites.isEmpty()) {
            pendingWrites.removeAndFailAll(new ClosedChannelException());
        }
        ctx.fireChannelInactive();
    }

    static void addHttp1Codec(ChannelPipeline pipeline, HttpProxyServerConfig serverConfig) {
        pipeline.addLast("httpCodec", new HttpClientCodec(
                serverConfig.getMaxInitialLineLength(),
                serverConfig.getMaxHeaderSize(),
                serverConfig.getMaxChunkSize()));
    }

    private static void addHttp1Codec(ChannelPipeline pipeline, String before,
                                      HttpProxyServerConfig serverConfig) {
        pipeline.addBefore(before, "httpCodec", new HttpClientCodec(
                serverConfig.getMaxInitialLineLength(),
                serverConfig.getMaxHeaderSize(),
                serverConfig.getMaxChunkSize()));
    }

    private static void addHttp2Codec(ChannelPipeline pipeline, String before) {
        final Http2Connection connection = new DefaultHttp2Connection(false);
        final InboundHttp2ToHttpAdapter inboundAdapter = new InboundHttp2ToHttpAdapterBuilder(connection)
                .maxContentLength(MAX_HTTP2_CONTENT_LENGTH)
                .validateHttpHeaders(true)
                .build();
        final Http2Settings settings = new Http2Settings()
                .headerTableSize(65536L)
                .pushEnabled(false)
                .initialWindowSize(6291456)
                .maxHeaderListSize(262144L);
        final ChromeHttpToHttp2ConnectionHandler codec =
                new ChromeHttpToHttp2ConnectionHandler.Builder()
                .connection(connection)
                .frameListener(inboundAdapter)
                .initialSettings(settings)
                .httpScheme(HttpScheme.HTTPS)
                .build();

        pipeline.addBefore(before, "http2Codec", codec);
        try {
            codec.decoder().flowController().incrementWindowSize(
                    connection.connectionStream(), 15663105);
        } catch (Http2Exception e) {
            throw new IllegalStateException("Unable to configure the Chrome HTTP/2 receive window", e);
        }
        pipeline.addBefore(before, "http2Decompress",
                new HttpContentDecompressor(MAX_HTTP2_CONTENT_LENGTH));
        pipeline.addBefore(before, "http2Aggregator",
                new HttpObjectAggregator(MAX_HTTP2_CONTENT_LENGTH));
    }
}
