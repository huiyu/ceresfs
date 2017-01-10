package com.supconit.ceresfs.http;

import com.supconit.ceresfs.storage.ImageDirectory;
import com.supconit.ceresfs.storage.ImageStore;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;
import com.supconit.ceresfs.util.HttpUtil;

import org.springframework.beans.factory.annotation.Autowired;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

public abstract class AbstractHttpResponder implements HttpResponder {

    private final Topology topology;
    private final HttpClientPool httpClientPool;
    private final ImageDirectory directory;
    private final ImageStore store;

    @Autowired
    public AbstractHttpResponder(Topology topology,
                                 HttpClientPool httpClientPool,
                                 ImageDirectory directory,
                                 ImageStore store) {
        this.topology = topology;
        this.httpClientPool = httpClientPool;
        this.directory = directory;
        this.store = store;
    }

    protected Topology getTopology() {
        return topology;
    }

    protected HttpClientPool getHttpClientPool() {
        return httpClientPool;
    }

    protected ImageDirectory getDirectory() {
        return directory;
    }

    protected ImageStore getStore() {
        return store;
    }

    /**
     * Http forward
     */
    protected void forward(Node node, ChannelHandlerContext ctx, FullHttpRequest req)
            throws Exception {
        HttpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                .newCall(req.copy())
                .whenComplete((res, ex) -> {
                    HttpResponse response = ex != null ?
                            res.copy() :
                            HttpUtil.newResponse(INTERNAL_SERVER_ERROR, ex.getMessage());
                    ctx.writeAndFlush(response);
                });
    }
}
