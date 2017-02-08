package com.supconit.ceresfs.http;

import com.supconit.ceresfs.storage.Image;

import java.io.File;
import java.util.Map;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;

public class ImageUploadClient {

    private HttpClient client;

    public ImageUploadClient(String host, int port) {
        client = new HttpClient(host, port);
    }

    public void upload(String uri, File file, Image.Type type, Map<String, String> attributes) throws Exception {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
        HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, true);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            encoder.addBodyAttribute(entry.getKey(), entry.getValue());
        }
        encoder.addBodyFileUpload("file", file, type.getMimeType(), false);
        encoder.finalizeRequest();

        HttpContent content;
        while ((content = encoder.readChunk((ByteBufAllocator) null)) != null) {
            request.content().writeBytes(content.content());
        }
        client.newCall(request);
    }

    public void upload(String uri, Image image) throws Exception {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
        HttpDataFactory factory = new DefaultHttpDataFactory();
        HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(factory, request, true);

        Image.Index index = image.getIndex();
        encoder.addBodyAttribute("id", String.valueOf(index.getId()));
        encoder.addBodyAttribute("expireTime", String.valueOf(index.getExpireTime()));

        FileUpload fileUpload = factory.createFileUpload(
                request,
                "file",
                "test." + index.getType().getFileSuffix(),
                "application/octet-stream",
                "binary",
                null,
                image.getData().length);
        fileUpload.setContent(Unpooled.wrappedBuffer(image.getData()));
        encoder.addBodyHttpData(fileUpload);
        encoder.finalizeRequest();

        HttpContent content;
        while ((content = encoder.readChunk((ByteBufAllocator) null)) != null) {
            request.content().writeBytes(content.content());
        }
        client.newCall(request);
    }

    public void shutdown() {
        this.client.shutdown();
    }
}
