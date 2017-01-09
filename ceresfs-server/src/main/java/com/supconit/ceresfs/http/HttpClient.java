package com.supconit.ceresfs.http;

import com.supconit.ceresfs.CeresFS;
import com.supconit.ceresfs.config.Configuration;
import com.supconit.ceresfs.exception.CeresFSException;

import org.springframework.util.Assert;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

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

public class HttpClient {

    private final String host;
    private final int port;

    private volatile EventLoopGroup workerGroup;
    private volatile Channel channel;
    private volatile HttpClientHandler clientHandler;

    public HttpClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.clientHandler = new HttpClientHandler();
        this.workerGroup = new NioEventLoopGroup();

        // aggregator buffer size must greater than the max image size
        int aggregatorBufferSize = CeresFS.getContext().getBean(Configuration.class)
                .getImageMaxSize() + 8192;

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
            throw new CeresFSException(e);
        }
    }


    public CompletableFuture<FullHttpResponse> newCall(HttpRequest request) {
        CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();
        this.clientHandler.offer(future);
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
    }

    protected void writeAndFlush(Object msg) {
        Assert.notNull(msg);
        channel.writeAndFlush(msg);
    }

    @ChannelHandler.Sharable
    static class HttpClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

        private ConcurrentLinkedQueue<CompletableFuture<FullHttpResponse>> queue = new ConcurrentLinkedQueue<>();

        public void offer(CompletableFuture<FullHttpResponse> future) {
            this.queue.offer(future);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {

            CompletableFuture<FullHttpResponse> future = queue.poll();
            if (future == null) {
                // TODO
            } else {
                future.complete(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            queue.poll().completeExceptionally(cause);
        }
    }
}
