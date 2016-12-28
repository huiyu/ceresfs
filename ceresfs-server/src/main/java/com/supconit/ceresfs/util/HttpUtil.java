package com.supconit.ceresfs.util;

import java.io.PrintWriter;
import java.io.StringWriter;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class HttpUtil {

    private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;
    private static final String MIME_PLAIN = "text/plain;";

    public static HttpResponse newResponse(HttpResponseStatus status, String message) {
        return newResponse(status, message.getBytes());
    }

    public static HttpResponse newResponse(HttpResponseStatus status, byte[] data) {
        return newResponse(status, MIME_PLAIN, data);
    }

    public static HttpResponse newResponse(HttpResponseStatus status, String mimeType, String message) {
        return newResponse(status, mimeType, message.getBytes());
    }

    public static HttpResponse newResponse(HttpResponseStatus status, Throwable cause) {
        StringWriter writer = new StringWriter();
        cause.printStackTrace(new PrintWriter(writer));
        return HttpUtil.newResponse(status, writer.toString());
    }

    public static HttpResponse newResponse(HttpResponseStatus status, String mimeType, byte[] data) {
        DefaultFullHttpResponse response =
                new DefaultFullHttpResponse(VERSION, status, Unpooled.wrappedBuffer(data));
        response.headers()
                .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
                .set(HttpHeaderNames.CONTENT_TYPE, mimeType);
        return response;
    }
}
