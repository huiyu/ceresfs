package com.supconit.ceresfs.http;

import com.supconit.ceresfs.storage.Image;

import sun.security.jgss.LoginConfigImpl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
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
        client.newCall(request).execute();
    }

    public void shutdown() {
        this.client.shutdown();
    }

    public static void main(String[] args) throws Exception {
        String path = ImageUploadClient.class.getResource("/669006.png").getFile();
        File file = new File(path);

        ImageUploadClient client = new ImageUploadClient("localhost", 9900);
        for (int i = 0; i < 1000; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("id", String.valueOf(i));
            client.upload("/image", file, Image.Type.PNG, attributes); 
        }
    }
}
