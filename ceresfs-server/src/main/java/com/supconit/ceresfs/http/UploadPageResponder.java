package com.supconit.ceresfs.http;

import com.supconit.ceresfs.util.HttpUtil;

import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Component
public class UploadPageResponder implements HttpResponder {

    private static final String MIME_HTML = "text/html;";

    @Override
    public String[] paths() {
        return new String[]{"/upload"};
    }

    @Override
    public HttpMethod[] methods() {
        return new HttpMethod[]{HttpMethod.GET};
    }

    @Override
    public void handle(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpResponse response = HttpUtil.newResponse(OK, MIME_HTML, getUploadHtml());
        context.writeAndFlush(response);
    }

    /**
     * TODO cache
     */
    private byte[] getUploadHtml() throws IOException {
        try (InputStream in = this.getClass().getResourceAsStream("/upload.html")) {
            return StreamUtils.copyToByteArray(in);
        }
    }
}
