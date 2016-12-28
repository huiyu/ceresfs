package com.supconit.ceresfs;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CeresFSServer {

    private static final Logger LOG = LoggerFactory.getLogger(CeresFSServer.class);
    private static final int AGGREGATOR_BUFFER_SIZE = 512 * 1024;

    private final int port;
    private final CeresFSServerHandler handler;
    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    @Autowired
    public CeresFSServer(CeresFSConfiguration configuration, CeresFSServerHandler handler) {
        this.port = configuration.getPort();
        this.handler = handler;
    }

    public void start() throws InterruptedException {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast("encoder", new HttpResponseEncoder())
                                .addLast("decoder", new HttpRequestDecoder())
                                .addLast("aggregator", new HttpObjectAggregator(AGGREGATOR_BUFFER_SIZE))
                                .addLast(handler);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        ChannelFuture f = bootstrap.bind(port).sync();
        LOG.info("CeresFS started at port at {}", port);
        f.channel().closeFuture().sync();
    }

    public void shutdown() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
}
