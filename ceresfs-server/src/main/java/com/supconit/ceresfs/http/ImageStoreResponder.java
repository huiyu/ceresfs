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
import org.springframework.stereotype.Component;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Component
public class ImageStoreResponder implements HttpResponder {

    static {
        // delete file on exist (normal exist)
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskAttribute.deleteOnExitTemporaryFile = true;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ImageStoreResponder.class);

    private static final DefaultHttpDataFactory USE_MEMORY = new DefaultHttpDataFactory(false);

    private final Topology topology;
    private final HttpClientPool httpClientPool;
    private final ImageDirectory directory;
    private final ImageStore store;

    public ImageStoreResponder(Topology topology,
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
        return new HttpMethod[]{HttpMethod.POST};
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(USE_MEMORY, req);
        try {
            ImageStoreRequestResolver resolver = new ImageStoreRequestResolver(decoder);
            if (resolver.hasError()) {
                ctx.writeAndFlush(resolver.getErrorResponse());
                return;
            }

            Disk disk = topology.route(resolver.getImageId());
            Node node = disk.getNode();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Image {} route to {}:{}:{}",
                        resolver.getImageId(),
                        node.getHostAddress(),
                        node.getPort(),
                        disk.getPath());
            }

            if (!node.equals(topology.localNode())) { // redirect
                HttpClient client = httpClientPool.borrowObject(node);
                client.newCall(req.copy()).whenComplete((res, ex) -> {
                    httpClientPool.returnObject(node, client);
                    HttpResponse response = ex != null ?
                            res.copy() :
                            HttpUtil.newResponse(INTERNAL_SERVER_ERROR, ex.getMessage());
                    ctx.writeAndFlush(response);
                });
                return;
            }

            // image id existence check
            if (directory.contains(disk, resolver.getImageId())) {
                ctx.writeAndFlush(HttpUtil.newResponse(BAD_REQUEST,
                        "Image[id=" + resolver.getImageId() + "] already exist"));
                return;
            }

            store.save(disk, resolver.getImageId(), resolver.getImageType(), resolver.getImageData())
                    .thenAccept(image -> {
                        directory.save(disk, image.getIndex());
                        ctx.writeAndFlush(HttpUtil.newResponse(OK, OK.reasonPhrase()));
                    })
                    .exceptionally(ex -> {
                        ctx.writeAndFlush(HttpUtil.newResponse(INTERNAL_SERVER_ERROR, ex));
                        return null;
                    });
        } finally {
            decoder.destroy();
        }
    }

    private static class ImageStoreRequestResolver {

        private HttpResponse errorResponse;

        private long imageId;
        private Image.Type imageType;
        private long imageExpireTime;
        private boolean sync;
        private byte[] imageData;

        public ImageStoreRequestResolver(HttpPostRequestDecoder decoder) throws IOException {
            // check id
            InterfaceHttpData idData = decoder.getBodyHttpData("id");
            if (idData == null) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST, "No image id.");
                return;
            }

            if (!(idData instanceof Attribute)) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST, "Can't resolve image id.");
                return;
            }
            try {
                this.imageId = Long.parseLong(((Attribute) idData).getValue());
            } catch (NumberFormatException e) {
                this.errorResponse = HttpUtil.newResponse(
                        BAD_REQUEST,
                        "Image id " + ((Attribute) idData).getValue() + " is not long value.");
                return;
            }

            // read file
            InterfaceHttpData fileData = decoder.getBodyHttpData("file");
            if (fileData == null) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST, "No image file.");
                return;
            }
            if (!(fileData instanceof FileUpload)) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST, "Not multipart/form-data request.");
                return;
            }
            FileUpload fileUpload = (FileUpload) fileData;
            String fileName = fileUpload.getFilename();
            try {
                this.imageType = Image.Type.parse(fileName);
            } catch (IllegalArgumentException e) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST,
                        "File " + ((FileUpload) fileData).getFilename() + "is not a image.");
            }

            ByteBuf byteBuf = fileUpload.getByteBuf();
            this.imageData = byteBuf.array();

            // check expire time
            InterfaceHttpData expireTimeData = decoder.getBodyHttpData("expireTimeData");
            if (expireTimeData == null) {
                // -1 means never expire
                imageExpireTime = -1L;
            } else {
                try {
                    String expireTimeValue = ((Attribute) expireTimeData).getValue();
                    imageExpireTime = Long.valueOf(expireTimeValue);
                } catch (NumberFormatException e) {
                    this.errorResponse = HttpUtil.newResponse(
                            BAD_REQUEST,
                            "Expire time " + ((Attribute) expireTimeData).getValue() + " is not unix time stamp.");
                    return;
                }
            }

            // check sync
            InterfaceHttpData syncData = decoder.getBodyHttpData("sync");
            if (syncData != null && ((Attribute) syncData).getValue().equalsIgnoreCase("true")) {
                sync = true;
            } else {
                sync = false;
            }
        }

        public boolean hasError() {
            return errorResponse != null;
        }

        public HttpResponse getErrorResponse() {
            return errorResponse;
        }

        public long getImageId() {
            return imageId;
        }

        public Image.Type getImageType() {
            return imageType;
        }

        public byte[] getImageData() {
            return imageData;
        }

        public long getImageExpireTime() {
            return imageExpireTime;
        }

        public boolean isSync() {
            return sync;
        }
    }
}
