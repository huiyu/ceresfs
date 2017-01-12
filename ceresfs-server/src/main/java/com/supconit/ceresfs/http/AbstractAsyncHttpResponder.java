package com.supconit.ceresfs.http;

import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.util.HttpUtil;

import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

public abstract class AbstractAsyncHttpResponder implements HttpResponder {

    public static final String MSG_FORWARD_FORBIDDEN = "Request forward is not allowed";

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
        int maxForwards = maxForwardOf(req, 1);
        if (maxForwards <= 0) {
            FullHttpResponse resp = HttpUtil.newResponse(FORBIDDEN, MSG_FORWARD_FORBIDDEN);
            return CompletableFuture.completedFuture(resp);
        }

        FullHttpRequest copy = req.copy();
        copy.headers().set(HttpHeaderNames.MAX_FORWARDS, maxForwards - 1);
        return HttpClientPool.getOrCreate(node.getHostAddress(), node.getPort()).newCall(copy);
    }

    protected int maxForwardOf(FullHttpRequest req, int defaultValue) {
        String headerMaxForwards = req.headers().get(HttpHeaderNames.MAX_FORWARDS);
        int maxForwards = defaultValue;
        if (headerMaxForwards != null) {
            maxForwards = Integer.valueOf(headerMaxForwards);
        }
        return maxForwards;
    }

    protected abstract CompletableFuture<FullHttpResponse> getResponse(FullHttpRequest req);
}
