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

import java.util.concurrent.CompletableFuture;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Component
public class ImageDeletionResponder extends AbstractAsyncHttpResponder {

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
    protected CompletableFuture<FullHttpResponse> getResponse(FullHttpRequest req) {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(USE_MEMORY, req);
        try {

            InterfaceHttpData idData = decoder.getBodyHttpData("id");
            if (idData == null) {
                FullHttpResponse resp = HttpUtil.newResponse(BAD_REQUEST, "No image id");
                return CompletableFuture.completedFuture(resp);
            }

            if (!(idData instanceof Attribute)) {
                FullHttpResponse resp = HttpUtil.newResponse(BAD_REQUEST,
                        "Can't resolve image id.");
                return CompletableFuture.completedFuture(resp);
            }

            try {
                long id = Long.parseLong(((Attribute) idData).getValue());

                Disk disk = this.topology.route(id);
                Node node = disk.getNode();

                if (!node.equals(topology.localNode())) {
                    return forward(node, req);
                }

                Image.Index index = directory.get(disk, id);
                store.delete(disk, index);
                directory.delete(disk, id);
                return CompletableFuture.completedFuture(HttpUtil.newResponse(OK));
            } catch (NumberFormatException e) {
                return CompletableFuture.completedFuture(HttpUtil.newResponse(
                        BAD_REQUEST, "Image id is not long value."));
            } catch (Exception e) {
                CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        } finally {
            decoder.destroy();
        }
    }
}
