package com.supconit.ceresfs.http;

import org.springframework.stereotype.Component;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;

@Component
public class ImageDeletionResponder implements HttpResponder {

    @Override
    public String[] paths() {
        return new String[]{"/image"};
    }

    @Override
    public HttpMethod[] methods() {
        return new HttpMethod[]{HttpMethod.DELETE};
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        System.out.println(req);
    }
}
