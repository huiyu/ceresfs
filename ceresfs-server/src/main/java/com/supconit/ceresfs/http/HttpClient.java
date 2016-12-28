package com.supconit.ceresfs.http;

import com.supconit.ceresfs.EventHandler;
import com.supconit.ceresfs.exception.CeresFSException;

import org.springframework.util.Assert;

import java.util.concurrent.ConcurrentLinkedQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;

public class HttpClient {

    private static final int AGGREGATOR_BUFFER_SIZE = 512 * 1024;

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
                    ch.pipeline().addLast(new HttpObjectAggregator(AGGREGATOR_BUFFER_SIZE));
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

    public HttpRequestBuilder newCall(FullHttpRequest request) {
        return new HttpRequestBuilder(request, this);
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


    public void putHandler(EventHandler<FullHttpResponse> responseHandler,
                           EventHandler<Throwable> exceptionHandler) {
        this.clientHandler.offer(responseHandler, exceptionHandler);
    }

    public static class HttpRequestBuilder {

        private FullHttpRequest request;
        private EventHandler<FullHttpResponse> onCompletion = r -> {
        }; // FIXME: do nothing?
        private EventHandler<Throwable> onException = Throwable::printStackTrace;

        private HttpClient client;

        public HttpRequestBuilder(FullHttpRequest request, HttpClient client) {
            this.request = request;
            this.client = client;
        }

        public HttpRequestBuilder content(byte[] content) {
            request = request.replace(Unpooled.wrappedBuffer(content));
            return this;
        }

        public HttpRequestBuilder setHeader(String key, Object value) {
            request.headers().set(key, value);
            return this;
        }

        public HttpRequestBuilder setHeader(String key, Iterable<?> values) {
            request.headers().set(key, values);
            return this;
        }

        public HttpRequestBuilder onSuccess(EventHandler<FullHttpResponse> responseHandler) {
            Assert.notNull(responseHandler);
            this.onCompletion = responseHandler;
            return this;
        }

        public HttpRequestBuilder onError(EventHandler<Throwable> exceptionHandler) {
            Assert.notNull(exceptionHandler);
            this.onException = exceptionHandler;
            return this;
        }

        public void execute() {
            client.putHandler(onCompletion, onException);
            client.writeAndFlush(request);
        }
    }

    @ChannelHandler.Sharable
    static class HttpClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

        private ConcurrentLinkedQueue<HttpEventHandler> queue = new ConcurrentLinkedQueue<>();

        public void offer(EventHandler<FullHttpResponse> responseSubscriber,
                          EventHandler<Throwable> exceptionHandler) {
            this.queue.offer(new HttpEventHandler(responseSubscriber, exceptionHandler));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
            HttpEventHandler httpEventHandler = queue.poll();
            if (httpEventHandler == null) {
                // TODO 
                return;
            }
            try {
                httpEventHandler.getResponseHandler().handle(msg);
            } catch (Throwable e) {
                httpEventHandler.getExceptionHandler().handle(e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            queue.poll().getExceptionHandler().handle(cause);
        }
    }
}
