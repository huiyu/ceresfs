package com.supconit.ceresfs.http;

import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.util.HttpUtil;

import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public abstract class AbstractAsyncHttpResponder implements HttpResponder {

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        this.getResponse(req).whenComplete((resp, ex) -> {
            if (ex != null) {
                HttpUtil.newResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
                ctx.writeAndFlush(ex);
            } else {
                String id = req.headers().get("id");
                if (id != null) {
                    req.headers().set("id", id);
                }
                ctx.writeAndFlush(resp);
            }
        });
    }


    protected CompletableFuture<FullHttpResponse> forward(Node node, FullHttpRequest req) {
        String headerMaxForwards = req.headers().get(HttpHeaderNames.MAX_FORWARDS);
        int maxForwards = 1;
        if (headerMaxForwards != null) {
            maxForwards = Integer.valueOf(headerMaxForwards);
            if (maxForwards <= 0)
                return CompletableFuture.completedFuture(HttpUtil.newResponse(
                        HttpResponseStatus.FORBIDDEN, "Request forward is not allowed"));
        }

        FullHttpRequest copy = req.copy();
        copy.headers().set(HttpHeaderNames.MAX_FORWARDS, maxForwards - 1);
        return HttpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                .newCall(copy);
    }

    protected abstract CompletableFuture<FullHttpResponse> getResponse(FullHttpRequest req);
}
