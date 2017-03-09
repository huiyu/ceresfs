package io.github.huiyu.ceresfs.http;

import io.github.huiyu.ceresfs.Const;
import io.github.huiyu.ceresfs.topology.Node;
import io.github.huiyu.ceresfs.util.HttpUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

public abstract class AbstractAsyncHttpResponder implements HttpResponder {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncHttpResponder.class);

    protected static final String MSG_FORWARD_FORBIDDEN = "Request forward is not allowed";

    @Autowired
    protected HttpClientPool httpClientPool;

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        this.getResponse(req).whenComplete((resp, ex) -> {
            if (ex != null) {
                LOG.error("Internal server error", ex);
                ctx.writeAndFlush(HttpUtil.newResponse(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR, ex));
            } else {
                String token = req.headers().get(Const.HTTP_TOKEN_NAME);
                if (token != null) {
                    resp.headers().set(Const.HTTP_TOKEN_NAME, token);
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
        copy.headers().set(Const.HTTP_HEADER_MAX_FORWARDS, maxForwards - 1);
        return httpClientPool.getOrCreate(node.getHostAddress(), node.getPort()).newCall(copy);
    }

    protected int maxForwardOf(FullHttpRequest req, int defaultValue) {
        String headerMaxForwards = req.headers().get(Const.HTTP_HEADER_MAX_FORWARDS);
        int maxForwards = defaultValue;
        if (headerMaxForwards != null) {
            maxForwards = Integer.valueOf(headerMaxForwards);
        }
        return maxForwards;
    }

    protected CompletableFuture<FullHttpResponse> broadcast(Collection<Node> nodes,
                                                            FullHttpRequest req) {
        // check http max-forward
        int maxForwardOf = maxForwardOf(req, 1);
        if (maxForwardOf <= 0) {
            FullHttpResponse resp = HttpUtil.newResponse(FORBIDDEN, MSG_FORWARD_FORBIDDEN);
            return CompletableFuture.completedFuture(resp);
        }

        // copy request
        final int size = nodes.size();
        final ConcurrentLinkedQueue<FullHttpRequest> requests = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < size; i++) {
            requests.offer(req.copy());
        }
        // do broadcast
        final ForkJoinPool pool = ForkJoinPool.commonPool();
        return CompletableFuture.supplyAsync(() -> {
            final AtomicReference<FullHttpResponse> ref = new AtomicReference<>();
            final CountDownLatch completeOne = new CountDownLatch(1);
            pool.submit(() -> {
                final CountDownLatch completeAll = new CountDownLatch(size);
                for (Node node : nodes) {
                    try {
                        CompletableFuture<FullHttpResponse> forward = forward(node, requests.poll());
                        FullHttpResponse response = forward.get();
                        ref.set(response);
                        completeOne.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        completeAll.countDown();
                    }
                }
                try {
                    completeAll.await();
                } catch (InterruptedException e) {
                } finally {
                    completeOne.countDown();
                }
            });

            try {
                completeOne.await();
            } catch (InterruptedException e) {
            }

            FullHttpResponse response = ref.get();
            if (response == null) {
                response = HttpUtil.newResponse(NOT_FOUND);
            }
            return response;
        });
    }


    protected abstract CompletableFuture<FullHttpResponse> getResponse(FullHttpRequest req);
}
