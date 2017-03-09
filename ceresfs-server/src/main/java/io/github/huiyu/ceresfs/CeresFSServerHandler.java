package io.github.huiyu.ceresfs;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import io.github.huiyu.ceresfs.http.HttpResponder;
import io.github.huiyu.ceresfs.util.HttpUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Map;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

@Component
@ChannelHandler.Sharable
public class CeresFSServerHandler extends SimpleChannelInboundHandler<FullHttpRequest>
        implements ApplicationContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(CeresFSServerHandler.class);

    private Table<String, HttpMethod, HttpResponder> routeTable = HashBasedTable.create();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        HttpMethod method = msg.method();
        String path = new QueryStringDecoder(msg.uri()).path();

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} {} {}", method, msg.uri(), msg.protocolVersion().text());
        }

        HttpResponder requestHandler = routeTable.get(path, method);
        if (requestHandler == null) {
            // 404
            HttpResponse response = HttpUtil.newResponse(NOT_FOUND, NOT_FOUND.reasonPhrase());
            ctx.writeAndFlush(response);
        } else {
            requestHandler.handle(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Internal server error, ", cause);
        ctx.writeAndFlush(HttpUtil.newResponse(INTERNAL_SERVER_ERROR, cause));
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        Map<String, HttpResponder> handlerByName = ctx.getBeansOfType(HttpResponder.class);
        for (HttpResponder handler : handlerByName.values()) {
            Assert.isTrue(!StringUtils.isEmpty(handler.paths()),
                    "Handler path can't be null or empty.");
            for (String path : handler.paths()) {
                HttpMethod[] methods = handler.methods();
                if (methods == null || methods.length == 0) {
                    methods = HttpMethod.class.getEnumConstants();
                }

                for (HttpMethod method : methods) {
                    HttpResponder exist = routeTable.get(path, method);
                    if (exist != null) {
                        throw new BeanInitializationException("Multiple http responder of " +
                                "[method=" + method + ",path=" + path + "]: \n" +
                                exist.getClass().getName() + "\n" +
                                handler.getClass().getName());
                    }
                    LOG.debug("Load http handler {}.", handler.getClass().getName());
                    routeTable.put(path, method, handler);
                }
            }
        }
    }
}
