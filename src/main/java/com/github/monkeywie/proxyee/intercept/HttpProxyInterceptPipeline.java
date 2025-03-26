package com.github.monkeywie.proxyee.intercept;

import com.github.monkeywie.proxyee.proxy.ProxyConfig;
import com.github.monkeywie.proxyee.util.ProtoUtil.RequestProto;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class HttpProxyInterceptPipeline implements Iterable<HttpProxyIntercept> {

    private List<HttpProxyIntercept> intercepts;

    private int posBeforeConnect = 0;
    private int posBeforeHead = 0;
    private int posBeforeContent = 0;
    private int posAfterHead = 0;
    private int posAfterContent = 0;
    private int posWebsocketHandshake = 0;
    private int posWebsocketRequest = 0;
    private int posWebsocketResponse = 0;
    private int posWebsocketClose = 0;

    private RequestProto requestProto;
    private HttpRequest httpRequest;
    private HttpResponse httpResponse;
    private ProxyConfig proxyConfig;

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public RequestProto getRequestProto() {
        return requestProto;
    }

    public ProxyConfig getProxyConfig() {
        return proxyConfig;
    }

    public void setRequestProto(RequestProto requestProto) {
        this.requestProto = requestProto;
    }

    public void setProxyConfig(ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }

    public HttpProxyInterceptPipeline(HttpProxyIntercept defaultIntercept) {
        this.intercepts = new LinkedList<>();
        this.intercepts.add(defaultIntercept);
    }

    public void addLast(HttpProxyIntercept intercept) {
        this.intercepts.add(this.intercepts.size() - 1, intercept);
    }

    public void addFirst(HttpProxyIntercept intercept) {
        this.intercepts.add(0, intercept);
    }

    public HttpProxyIntercept get(int index) {
        return this.intercepts.get(index);
    }

    public void remove(HttpProxyIntercept intercept) {
        this.intercepts.remove(intercept);
    }

    public void beforeConnect(Channel clientChannel) throws Exception {
        if (this.posBeforeConnect < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posBeforeConnect++);
            intercept.beforeConnect(clientChannel, this);
        }
        this.posBeforeConnect = 0;
    }

    public void beforeRequest(Channel clientChannel, HttpRequest httpRequest) throws Exception {
        this.httpRequest = httpRequest;
        if (this.posBeforeHead < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posBeforeHead++);
            intercept.beforeRequest(clientChannel, this.httpRequest, this);
        }
        this.posBeforeHead = 0;
    }

    public void beforeRequest(Channel clientChannel, HttpContent httpContent) throws Exception {
        if (this.posBeforeContent < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posBeforeContent++);
            intercept.beforeRequest(clientChannel, httpContent, this);
        }
        this.posBeforeContent = 0;
    }

    public void afterResponse(Channel clientChannel, Channel proxyChannel, HttpResponse httpResponse)
            throws Exception {
        this.httpResponse = httpResponse;
        if (this.posAfterHead < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posAfterHead++);
            intercept.afterResponse(clientChannel, proxyChannel, this.httpResponse, this);
        }
        this.posAfterHead = 0;
    }

    public void afterResponse(Channel clientChannel, Channel proxyChannel, HttpContent httpContent)
            throws Exception {
        if (this.posAfterContent < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posAfterContent++);
            intercept.afterResponse(clientChannel, proxyChannel, httpContent, this);
        }
        this.posAfterContent = 0;
    }

    public void websocketHandshakeCompleted() {
        if (this.posWebsocketHandshake < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posWebsocketHandshake++);
            intercept.onWebsocketHandshakeCompleted(this);
        }
        this.posWebsocketHandshake = 0;
    }

    public void websocketRequest(Channel clientChannel, Channel proxyChannel, WebSocketFrame webSocketFrame)
            throws Exception {
        if (this.posWebsocketRequest < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posWebsocketRequest++);
            intercept.onWebsocketRequest(clientChannel, proxyChannel, webSocketFrame, this);
        }
        this.posWebsocketRequest = 0;
    }

    public void websocketResponse(Channel clientChannel, Channel proxyChannel, WebSocketFrame webSocketFrame)
            throws Exception {
        if (this.posWebsocketResponse < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posWebsocketResponse++);
            intercept.onWebsocketResponse(clientChannel, proxyChannel, webSocketFrame, this);
        }
        this.posWebsocketResponse = 0;
    }

    public void websocketClose() {
        if (this.posWebsocketClose < intercepts.size()) {
            HttpProxyIntercept intercept = intercepts.get(this.posWebsocketClose++);
            intercept.onWebsocketClose(this);
        }
        this.posWebsocketClose = 0;
    }

    public int posBeforeHead() {
        return this.posBeforeHead;
    }

    public int posBeforeContent() {
        return this.posBeforeContent;
    }

    public int posAfterHead() {
        return this.posAfterHead;
    }

    public int posAfterContent() {
        return this.posAfterContent;
    }

    public void posBeforeHead(int pos) {
        this.posBeforeHead = pos;
    }

    public void posBeforeContent(int pos) {
        this.posBeforeContent = pos;
    }

    public void posAfterHead(int pos) {
        this.posAfterHead = pos;
    }

    public void posAfterContent(int pos) {
        this.posAfterContent = pos;
    }

    public void posWebsocketHandshake(int pos) {
        this.posWebsocketHandshake = pos;
    }

    public void posWebsocketRequest(int pos) {
        this.posWebsocketRequest = pos;
    }

    public void posWebsocketResponse(int pos) {
        this.posWebsocketResponse = pos;
    }

    public void posWebsocketClose(int pos) {
        this.posWebsocketClose = pos;
    }

    public void resetBeforeHead() {
        posBeforeHead(0);
    }

    public void resetBeforeContent() {
        posBeforeContent(0);
    }

    public void resetAfterHead() {
        posAfterHead(0);
    }

    public void resetAfterContent() {
        posAfterContent(0);
    }

    public void resetWebsocketHandshake() {
        posWebsocketHandshake(0);
    }

    public void resetWebsocketRequest() {
        posWebsocketRequest(0);
    }

    public void resetWebsocketResponse() {
        posWebsocketResponse(0);
    }

    public void resetWebsocketClose() {
        posWebsocketClose(0);
    }

    @Override
    public Iterator<HttpProxyIntercept> iterator() {
        return intercepts.iterator();
    }
}
