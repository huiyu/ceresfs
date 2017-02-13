package com.supconit.ceresfs.util;

import com.supconit.ceresfs.Const;
import com.supconit.ceresfs.ImageType;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;

public class HttpUtil {

    private static final HttpDataFactory USE_MEMORY = new DefaultHttpDataFactory(false);

    private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;
    private static final String MIME_PLAIN = "text/plain;";
    private static final String DEFAULT_FILE_NAME = "file";

    public static FullHttpResponse newResponse(HttpResponseStatus status) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(VERSION, status);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        return response;
    }

    public static FullHttpResponse newResponse(HttpResponseStatus status, String message) {
        return newResponse(status, message.getBytes());
    }

    public static FullHttpResponse newResponse(HttpResponseStatus status, byte[] data) {
        return newResponse(status, MIME_PLAIN, data);
    }

    public static FullHttpResponse newResponse(HttpResponseStatus status,
                                               String mimeType,
                                               String message) {
        return newResponse(status, mimeType, message.getBytes());
    }

    public static FullHttpResponse newResponse(HttpResponseStatus status, Throwable cause) {
        StringWriter writer = new StringWriter();
        cause.printStackTrace(new PrintWriter(writer));
        return HttpUtil.newResponse(status, writer.toString());
    }

    public static FullHttpResponse newResponse(HttpResponseStatus status,
                                               String mimeType,
                                               byte[] data) {
        DefaultFullHttpResponse response =
                new DefaultFullHttpResponse(VERSION, status, Unpooled.wrappedBuffer(data));
        response.headers()
                .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
                .set(HttpHeaderNames.CONTENT_TYPE, mimeType);
        return response;
    }

    public static FullHttpRequest newRequest(HttpMethod method, String uri) {
        return new DefaultFullHttpRequest(VERSION, method, uri);
    }

    public static FullHttpRequest newRequest(HttpMethod method, String uri, byte[] content) {
        return new DefaultFullHttpRequest(VERSION, method, uri, Unpooled.wrappedBuffer(content));
    }

    public static FullHttpRequest newImageUploadRequest(long id,
                                                        ImageType type,
                                                        long expireTime,
                                                        byte[] data) throws Exception {
        FullHttpRequest request = newRequest(HttpMethod.POST, "/image");
        HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, true);
        encoder.addBodyAttribute(Const.HTTP_HEADER_EXPIRE_TIME, String.valueOf(expireTime));
        encoder.addBodyAttribute(Const.HTTP_HEADER_IMAGE_ID, String.valueOf(id));
        FileUpload fileUpload = USE_MEMORY.createFileUpload(
                request,
                DEFAULT_FILE_NAME,
                DEFAULT_FILE_NAME + "." + type.getFileSuffix(),
                type.getMimeType(),
                "binary",
                null,
                data.length);

        fileUpload.setContent(Unpooled.wrappedBuffer(data));
        encoder.addBodyHttpData(fileUpload);
        encoder.finalizeRequest();
        HttpContent content;
        while ((content = encoder.readChunk((ByteBufAllocator) null)) != null) {
            request.content().writeBytes(content.content());
        }
        return request;
    }

    public static FullHttpRequest newImageUploadRequest(ImageType type,
                                                        long expireTime,
                                                        byte[] data) throws Exception {
        FullHttpRequest request = newRequest(HttpMethod.POST, "/image");
        HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, true);
        encoder.addBodyAttribute(Const.HTTP_HEADER_EXPIRE_TIME, String.valueOf(expireTime));
        FileUpload fileUpload = USE_MEMORY.createFileUpload(
                request,
                DEFAULT_FILE_NAME,
                DEFAULT_FILE_NAME + "." + type.getFileSuffix(),
                type.getMimeType(),
                "binary",
                null,
                data.length);

        fileUpload.setContent(Unpooled.wrappedBuffer(data));
        encoder.addBodyHttpData(fileUpload);
        encoder.finalizeRequest();
        HttpContent content;
        while ((content = encoder.readChunk((ByteBufAllocator) null)) != null) {
            request.content().writeBytes(content.content());
        }
        return request;
    }
}
