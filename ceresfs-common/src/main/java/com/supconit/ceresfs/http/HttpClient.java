package com.supconit.ceresfs.http;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;


import com.supconit.ceresfs.Const;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;

import static com.google.common.base.Preconditions.*;

public class HttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(15);

    private final String host;
    private final int port;

    private volatile EventLoopGroup workerGroup;
    private volatile Channel channel;
    private volatile HttpClientHandler clientHandler;

    private final Cache<String, CompletableFuture<FullHttpResponse>> callbacks;

    private HttpClient(String host, int port, long timeoutMills, int aggregatorBufferSize) {
        this.host = host;
        this.port = port;
        this.clientHandler = new HttpClientHandler();
        this.workerGroup = new NioEventLoopGroup();

        RemovalListener<String, CompletableFuture<FullHttpResponse>> listener = n -> {
            if (n.getCause().equals(RemovalCause.EXPLICIT)) {
                return;
            } else if (n.getCause().equals(RemovalCause.EXPIRED)) {
                n.getValue().completeExceptionally(new TimeoutException());
            } else {
                n.getValue().completeExceptionally(new InterruptedException());
            }
        };

        this.callbacks = CacheBuilder.newBuilder()
                .expireAfterAccess(timeoutMills, TimeUnit.MILLISECONDS)
                .removalListener(listener)
                .build();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new HttpResponseDecoder());
                    ch.pipeline().addLast(new HttpRequestEncoder());
                    ch.pipeline().addLast(new HttpObjectAggregator(aggregatorBufferSize));
                    ch.pipeline().addLast(clientHandler);
                }
            });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();
            channel = f.channel();
        } catch (InterruptedException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    public CompletableFuture<FullHttpResponse> newCall(HttpRequest request) {
        String token = UUID.randomUUID().toString();
        request.headers().set(Const.HTTP_TOKEN_NAME, token);
        CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();
        this.callbacks.put(token, future);
        writeAndFlush(request);
        return future;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void shutdown() {
        workerGroup.shutdownGracefully();
        callbacks.asMap().forEach((String id, CompletableFuture<FullHttpResponse> callback) ->
                callback.completeExceptionally(new InterruptedException()));
    }

    protected void writeAndFlush(Object msg) {
        channel.writeAndFlush(checkNotNull(msg));
    }

    public static class Builder {

        private final String host;
        private final int port;

        private long timeout = DEFAULT_TIMEOUT;
        private int aggregateBufferSize = Const.MAX_IMAGE_SIZE + 8192;

        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public Builder timeout(long time, TimeUnit timeUnit) {
            this.timeout = timeUnit.toMillis(time);
            return this;
        }

        public Builder aggregateBufferSize(int aggregateBufferSize) {
            this.aggregateBufferSize = aggregateBufferSize;
            return this;
        }

        public HttpClient build() {
            return new HttpClient(host, port, timeout, aggregateBufferSize);
        }
    }

    @ChannelHandler.Sharable
    class HttpClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg)
                throws Exception {
            String token = msg.headers().get(Const.HTTP_TOKEN_NAME);
            if (token == null) {
                InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
                LOG.error("No response handler for message from ", address.getHostString());
            }

            CompletableFuture<FullHttpResponse> future = callbacks.getIfPresent(checkNotNull(token));
            if (future != null) {
                callbacks.invalidate(token);
                future.complete(msg.copy());
            } else {
                InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
                LOG.error("No response handler for message from ", address.getHostString());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOG.error("HttpClient internal error", cause);
        }
    }
}
