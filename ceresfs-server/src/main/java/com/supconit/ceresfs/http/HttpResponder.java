package com.supconit.ceresfs.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;

public interface HttpResponder {

    /**
     * Request paths
     * 
     * @return
     */
    String[] paths();

    /**
     * Request methods
     * 
     * @return
     */
    default HttpMethod[] methods() {
        return null;
    }

    /**
     * Handle http requests
     * 
     * @param ctx
     * @param req
     * @throws Exception
     */
    void handle(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception;
}
