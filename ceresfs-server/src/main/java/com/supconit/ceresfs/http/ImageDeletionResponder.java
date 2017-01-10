package com.supconit.ceresfs.http;

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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

@Component
public class ImageDeletionResponder implements HttpResponder {

    private static final Logger LOG = LoggerFactory.getLogger(ImageDeletionResponder.class);

    private static final DefaultHttpDataFactory USE_MEMORY = new DefaultHttpDataFactory(false);

    private final Topology topology;
    private final ImageDirectory directory;
    private final ImageStore store;

    @Autowired
    public ImageDeletionResponder(Topology topology, ImageDirectory directory, ImageStore store) {
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
        return new HttpMethod[]{HttpMethod.DELETE};
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(USE_MEMORY, req);
        InterfaceHttpData idData = decoder.getBodyHttpData("id");
        if (idData == null) {
            ctx.writeAndFlush(HttpUtil.newResponse(BAD_REQUEST, "No image id."));
            return;
        }

        if (!(idData instanceof Attribute)) {
            ctx.writeAndFlush(HttpUtil.newResponse(BAD_REQUEST, "Can't resolve image id."));
            return;
        }

        try {
            long id = Long.parseLong(((Attribute) idData).getValue());

            Disk disk = this.topology.route(id);
            Node node = disk.getNode();

            if (!node.equals(topology.localNode())) {
                HttpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                        .newCall(req.copy())
                        .whenComplete((res, ex) -> {
                            HttpResponse response = ex != null ?
                                    res.copy() :
                                    HttpUtil.newResponse(INTERNAL_SERVER_ERROR, ex.getMessage());
                            ctx.writeAndFlush(response);
                        });
                return;
            }

            Image.Index index = directory.get(disk, id);
            store.delete(disk, index);
            directory.delete(disk, id);
        } catch (NumberFormatException e) {
            ctx.writeAndFlush(HttpUtil.newResponse(
                    BAD_REQUEST,
                    "Image id " + ((Attribute) idData).getValue() + " is not long value."));
        }
    }
}
