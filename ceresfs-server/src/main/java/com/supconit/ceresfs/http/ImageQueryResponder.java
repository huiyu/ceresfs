package com.supconit.ceresfs.http;

import com.supconit.ceresfs.storage.Image;
import com.supconit.ceresfs.storage.ImageDirectory;
import com.supconit.ceresfs.storage.ImageStore;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;
import com.supconit.ceresfs.util.HttpUtil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Component
public class ImageQueryResponder implements HttpResponder {

    private Topology topology;
    private HttpClientPool httpClientPool;
    private ImageDirectory directory;
    private ImageStore store;

    @Autowired
    public ImageQueryResponder(Topology topology,
                               HttpClientPool httpClientPool,
                               ImageDirectory directory,
                               ImageStore store) {
        this.topology = topology;
        this.httpClientPool = httpClientPool;
        this.directory = directory;
        this.store = store;
    }

    @Override
    public String[] paths() {
        return new String[]{"/image"};
    }

    @Override
    public HttpMethod[] methods() {
        return new HttpMethod[]{HttpMethod.GET};
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(req.uri());
        Map<String, List<String>> parameters = decoder.parameters();
        List<String> ids = parameters.get("id");
        if (CollectionUtils.isEmpty(ids)) {
            ctx.writeAndFlush(HttpUtil.newResponse(BAD_REQUEST, "Image id is not provided."));
            return;
        }

        if (ids.size() > 1) {
            ctx.writeAndFlush(HttpUtil.newResponse(BAD_REQUEST, "Image id is not explicitly specified."));
            return;
        }
        try {
            long id = Long.valueOf(ids.get(0));
            Disk disk = topology.route(id);
            Node node = disk.getNode();
            if (!node.equals(topology.localNode())) { // redirect
                HttpClient client = httpClientPool.borrowObject(node);
                client.newCall(req.copy()).whenComplete((res, ex) -> {
                    httpClientPool.returnObject(node, client);
                    HttpResponse response = ex == null ?
                            HttpUtil.newResponse(INTERNAL_SERVER_ERROR, ex.getMessage()) :
                            res.copy();
                    ctx.writeAndFlush(response);
                });
                return;
            }

            Image.Index index = directory.get(disk, id);
            if (index == null) {
                ctx.writeAndFlush(HttpUtil.newResponse(NOT_FOUND, "Image[id=" + id + "] not found."));
                return;
            }
            Image image = store.get(disk, index);
            String mimeType = image.getIndex().getType().getMimeType();
            ctx.writeAndFlush(HttpUtil.newResponse(OK, mimeType, image.getData()));
        } catch (NumberFormatException e) {
            ctx.writeAndFlush(HttpUtil.newResponse(BAD_REQUEST, ids.get(0) + " can't cast to long."));
        }
    }
}
