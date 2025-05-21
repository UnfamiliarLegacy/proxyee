package com.github.monkeywie.proxyee.handler;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.DeflateFrameClientExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.PerMessageDeflateClientExtensionHandshaker;

import static io.netty.handler.codec.http.websocketx.extensions.compression.PerMessageDeflateServerExtensionHandshaker.MAX_WINDOW_SIZE;

@ChannelHandler.Sharable
public final class WebsocketCompressionHandler extends WebSocketClientExtensionHandler {

    public static final WebsocketCompressionHandler INSTANCE = new WebsocketCompressionHandler();

    private WebsocketCompressionHandler() {
        super(new PerMessageDeflateClientExtensionHandshaker(6, ZlibCodecFactory.isSupportingWindowSizeAndMemLevel(), MAX_WINDOW_SIZE, true, true),
                new DeflateFrameClientExtensionHandshaker(false),
                new DeflateFrameClientExtensionHandshaker(true));
    }

}