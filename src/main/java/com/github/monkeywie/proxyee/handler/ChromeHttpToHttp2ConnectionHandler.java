/*
 * Derived from Netty's HttpToHttp2ConnectionHandler, licensed under Apache-2.0.
 */
package com.github.monkeywie.proxyee.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.AbstractHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.EmptyHttp2Headers;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;

import java.util.Map;

final class ChromeHttpToHttp2ConnectionHandler extends Http2ConnectionHandler {

    private final boolean validateHeaders;
    private final HttpScheme httpScheme;
    private int currentStreamId;

    private ChromeHttpToHttp2ConnectionHandler(Http2ConnectionDecoder decoder,
                                                Http2ConnectionEncoder encoder,
                                                Http2Settings initialSettings,
                                                boolean validateHeaders,
                                                HttpScheme httpScheme) {
        super(decoder, encoder, initialSettings);
        this.validateHeaders = validateHeaders;
        this.httpScheme = httpScheme;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!(msg instanceof HttpMessage || msg instanceof HttpContent)) {
            ctx.write(msg, promise);
            return;
        }

        boolean release = true;
        final PromiseCombiner promises = new PromiseCombiner(ctx.executor());
        Throwable failure = null;
        try {
            final Http2ConnectionEncoder encoder = encoder();
            boolean endStream = false;

            if (msg instanceof HttpMessage) {
                final HttpMessage httpMessage = (HttpMessage) msg;
                currentStreamId = httpMessage.headers().getInt(
                        HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(),
                        connection().local().incrementAndGetNextStreamId());

                if (httpScheme != null && !httpMessage.headers().contains(
                        HttpConversionUtil.ExtensionHeaderNames.SCHEME.text())) {
                    httpMessage.headers().set(
                            HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), httpScheme.name());
                }

                final Http2Headers http2Headers = toChromeHeaders(httpMessage, validateHeaders);
                endStream = msg instanceof FullHttpMessage
                        && !((FullHttpMessage) msg).content().isReadable();
                writeHeaders(ctx, encoder, currentStreamId, httpMessage.headers(),
                        http2Headers, endStream, promises);
            }

            if (!endStream && msg instanceof HttpContent) {
                final HttpContent httpContent = (HttpContent) msg;
                HttpHeaders trailers = EmptyHttpHeaders.INSTANCE;
                Http2Headers http2Trailers = EmptyHttp2Headers.INSTANCE;
                boolean lastContent = false;

                if (msg instanceof LastHttpContent) {
                    lastContent = true;
                    trailers = ((LastHttpContent) msg).trailingHeaders();
                    http2Trailers = HttpConversionUtil.toHttp2Headers(trailers, validateHeaders);
                }

                endStream = lastContent && trailers.isEmpty();
                final ByteBuf content = httpContent.content();
                final ChannelPromise dataPromise = ctx.newPromise();
                promises.add(dataPromise);
                encoder.writeData(ctx, currentStreamId, content, 0, endStream,
                        dataPromise);
                release = false;

                if (!trailers.isEmpty()) {
                    writeHeaders(ctx, encoder, currentStreamId, trailers,
                            http2Trailers, true, promises);
                }
            }
        } catch (Throwable t) {
            onError(ctx, true, t);
            failure = t;
        } finally {
            if (release) {
                ReferenceCountUtil.release(msg);
            }
            if (failure == null) {
                promises.finish(promise);
            } else {
                promise.tryFailure(failure);
            }
        }
    }

    private static Http2Headers toChromeHeaders(HttpMessage message, boolean validateHeaders) {
        final Http2Headers converted = HttpConversionUtil.toHttp2Headers(message, validateHeaders);
        if (!(message instanceof HttpRequest)) {
            return converted;
        }

        final Http2Headers ordered = new DefaultHttp2Headers(validateHeaders, converted.size());
        add(ordered, Http2Headers.PseudoHeaderName.METHOD.value(), converted.method());
        add(ordered, Http2Headers.PseudoHeaderName.AUTHORITY.value(), converted.authority());
        add(ordered, Http2Headers.PseudoHeaderName.SCHEME.value(), converted.scheme());
        add(ordered, Http2Headers.PseudoHeaderName.PATH.value(), converted.path());
        add(ordered, Http2Headers.PseudoHeaderName.PROTOCOL.value(),
                converted.get(Http2Headers.PseudoHeaderName.PROTOCOL.value()));

        for (Map.Entry<CharSequence, CharSequence> header : converted) {
            if (!Http2Headers.PseudoHeaderName.isPseudoHeader(header.getKey())) {
                ordered.add(header.getKey(), header.getValue());
            }
        }
        return ordered;
    }

    private static void add(Http2Headers headers, CharSequence name, CharSequence value) {
        if (value != null) {
            headers.add(name, value);
        }
    }

    private static void writeHeaders(ChannelHandlerContext ctx,
                                     Http2ConnectionEncoder encoder,
                                     int streamId,
                                     HttpHeaders headers,
                                     Http2Headers http2Headers,
                                     boolean endStream,
                                     PromiseCombiner promises) {
        final int dependencyId = headers.getInt(
                HttpConversionUtil.ExtensionHeaderNames.STREAM_DEPENDENCY_ID.text(), 0);
        final short weight = headers.getShort(
                HttpConversionUtil.ExtensionHeaderNames.STREAM_WEIGHT.text(),
                Http2CodecUtil.DEFAULT_PRIORITY_WEIGHT);
        final ChannelPromise writePromise = ctx.newPromise();
        promises.add(writePromise);
        encoder.writeHeaders(ctx, streamId, http2Headers, dependencyId, weight,
                false, 0, endStream, writePromise);
    }

    static final class Builder extends AbstractHttp2ConnectionHandlerBuilder<
            ChromeHttpToHttp2ConnectionHandler, Builder> {

        private HttpScheme httpScheme;

        public Builder connection(Http2Connection connection) {
            return super.connection(connection);
        }

        public Builder frameListener(Http2FrameListener frameListener) {
            return super.frameListener(frameListener);
        }

        public Builder initialSettings(Http2Settings settings) {
            return super.initialSettings(settings);
        }

        Builder httpScheme(HttpScheme httpScheme) {
            this.httpScheme = httpScheme;
            return this;
        }

        @Override
        protected ChromeHttpToHttp2ConnectionHandler build(
                Http2ConnectionDecoder decoder,
                Http2ConnectionEncoder encoder,
                Http2Settings initialSettings) {
            return new ChromeHttpToHttp2ConnectionHandler(
                    decoder, encoder, initialSettings, isValidateHeaders(), httpScheme);
        }

        @Override
        public ChromeHttpToHttp2ConnectionHandler build() {
            return super.build();
        }
    }
}
