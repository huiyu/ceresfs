package com.supconit.ceresfs.http;

import com.sun.tools.doclets.formats.html.SourceToHTMLConverter;
import com.supconit.ceresfs.storage.Image;
import com.supconit.ceresfs.storage.ImageDirectory;
import com.supconit.ceresfs.storage.ImageStore;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;
import com.supconit.ceresfs.util.HttpUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Component
public class ImageQueryResponder implements HttpResponder {

    private Topology topology;
    private ImageDirectory directory;
    private ImageStore store;

    @Autowired
    public ImageQueryResponder(Topology topology, ImageDirectory directory, ImageStore store) {
        this.topology = topology;
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
                HttpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                        .newCall(req.copy())
                        .onSuccess(r -> ctx.writeAndFlush(r.copy()))
                        .onError(e -> ctx.writeAndFlush(
                                HttpUtil.newResponse(INTERNAL_SERVER_ERROR, e.getMessage())))
                        .execute();
                return;
            }

            long t1 = System.currentTimeMillis();
            Image.Index index = directory.get(disk, id);
            long t2 = System.currentTimeMillis();
            if (index == null) {
                // FIXME: maybe in store's buffer
                ctx.writeAndFlush(HttpUtil.newResponse(NOT_FOUND, "Image[id=" + id + "] not found."));
                return;
            }
            Image image = store.get(disk, index);
            String mimeType = image.getIndex().getType().getMimeType();
            long t3 = System.currentTimeMillis();
            System.out.println("Index: " + (t2 - t1));
            System.out.println("Data: " + (t3 - t2));
            ctx.writeAndFlush(HttpUtil.newResponse(OK, mimeType, image.getData()));
        } catch (NumberFormatException e) {
            ctx.writeAndFlush(HttpUtil.newResponse(BAD_REQUEST, ids.get(0) + " can't cast to long."));
        }
    }
}
