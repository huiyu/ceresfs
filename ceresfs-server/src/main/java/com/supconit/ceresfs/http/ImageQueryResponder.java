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
import java.util.concurrent.CompletableFuture;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Component
public class ImageQueryResponder extends AbstractAsyncHttpResponder {

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
    protected CompletableFuture<FullHttpResponse> getResponse(FullHttpRequest req) {
        QueryStringDecoder decoder = new QueryStringDecoder(req.uri());
        Map<String, List<String>> parameters = decoder.parameters();
        List<String> ids = parameters.get("id");
        if (CollectionUtils.isEmpty(ids)) {
            FullHttpResponse resp = HttpUtil.newResponse(BAD_REQUEST, "Image id is not provided.");
            return CompletableFuture.completedFuture(resp);
        }

        if (ids.size() > 1) {
            FullHttpResponse resp = HttpUtil.newResponse(
                    BAD_REQUEST, "Image id is not explicitly specified.");
            return CompletableFuture.completedFuture(resp);
        }

        try {
            long id = Long.valueOf(ids.get(0));
            Disk disk = topology.route(id);
            Node node = disk.getNode();

            // forward
            if (!node.equals(topology.localNode())) {
                return forward(node, req);
            }

            Image.Index index = directory.get(disk, id);
            if (index == null) {
                FullHttpResponse resp = HttpUtil.newResponse(
                        NOT_FOUND, "Image[id=" + id + "] not found.");
                return CompletableFuture.completedFuture(resp);
            }
            Image image = store.get(disk, index);
            String mimeType = image.getIndex().getType().getMimeType();
            FullHttpResponse resp = HttpUtil.newResponse(OK, mimeType, image.getData());
            return CompletableFuture.completedFuture(resp);
        } catch (NumberFormatException e) {
            FullHttpResponse resp = HttpUtil.newResponse(
                    BAD_REQUEST, ids.get(0) + " can't cast to long.");
            return CompletableFuture.completedFuture(resp);
        } catch (Exception e) {
            CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
}
