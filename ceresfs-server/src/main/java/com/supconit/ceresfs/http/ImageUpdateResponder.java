package com.supconit.ceresfs.http;

import com.supconit.ceresfs.Const;
import com.supconit.ceresfs.storage.Facade;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;
import com.supconit.ceresfs.util.HttpUtil;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.CompletableFuture;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static com.supconit.ceresfs.Const.*;


public class ImageUpdateResponder extends AbstractAsyncHttpResponder {

    private static final DefaultHttpDataFactory USE_MEMORY = new DefaultHttpDataFactory(false);

    private final HttpMethod[] methods = {HttpMethod.PUT};
    private final String[] paths = {"/image"};

    private final Topology topology;
    private final Facade facade;

    @Autowired
    public ImageUpdateResponder(Topology topology, Facade facade) {
        this.topology = topology;
        this.facade = facade;
    }

    @Override
    public HttpMethod[] methods() {
        return methods;
    }

    @Override
    public String[] paths() {
        return paths;
    }

    @Override
    protected CompletableFuture<FullHttpResponse> getResponse(FullHttpRequest req) {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(USE_MEMORY, req);
        try {
            InterfaceHttpData idData = decoder.getBodyHttpData(Const.HTTP_HEADER_IMAGE_ID);
            if (idData == null) {
                FullHttpResponse resp = HttpUtil.newResponse(BAD_REQUEST, "No image id");
                return CompletableFuture.completedFuture(resp);
            }

            if (!(idData instanceof Attribute)) {
                FullHttpResponse resp = HttpUtil.newResponse(BAD_REQUEST, "Can't resolve image id.");
                return CompletableFuture.completedFuture(resp);
            }

            try {
                long id = Long.parseLong(((Attribute) idData).getValue());

                Disk disk = this.topology.route(id);
                Node node = disk.getNode();

                if (!topology.isLocalNode(node)) {
                    return forward(node, req);
                }

                Attribute expireTimeData = (Attribute) decoder.getBodyHttpData(HTTP_HEADER_EXPIRE_TIME);
                long expireTime = Long.parseLong(expireTimeData.getValue());

                // TODO update
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
