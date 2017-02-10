package com.supconit.ceresfs.http;

import com.google.common.util.concurrent.UncheckedExecutionException;

import com.supconit.ceresfs.snowflake.Snowflake;
import com.supconit.ceresfs.storage.Image;
import com.supconit.ceresfs.storage.Directory;
import com.supconit.ceresfs.storage.Store;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;
import com.supconit.ceresfs.util.HttpUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Component
public class ImageStoreResponder extends AbstractAsyncHttpResponder {

    static {
        // delete file on exist (normal exist)
        DiskFileUpload.deleteOnExitTemporaryFile = true;
        DiskAttribute.deleteOnExitTemporaryFile = true;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ImageStoreResponder.class);

    private static final DefaultHttpDataFactory USE_MEMORY = new DefaultHttpDataFactory(false);

    private final Topology topology;
    private final Directory directory;
    private final Store store;
    private final Snowflake snowflake;

    public ImageStoreResponder(Topology topology, Directory directory, Store store) {
        this.topology = topology;
        this.directory = directory;
        this.store = store;
        this.snowflake = new Snowflake.Builder(topology.getLocalNode().getId(), Short.SIZE).build();
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
    protected CompletableFuture<FullHttpResponse> getResponse(FullHttpRequest req) {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(USE_MEMORY, req);
        try {
            ImageStoreRequestResolver resolver = new ImageStoreRequestResolver(decoder, snowflake);
            if (resolver.hasError()) {
                return CompletableFuture.completedFuture(resolver.getErrorResponse());
            }

            Disk disk = topology.route(resolver.getImageId());
            Node node = disk.getNode();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Image {} route to {}:{}:{}", resolver.getImageId(),
                        node.getHostAddress(), node.getPort(), disk.getPath());
            }

//            if (!node.equals(topology.getLocalNode())) { // forward
            if (topology.isLocalNode(node)) {
                return forward(node, req);
            }

            // image id existence check
            if (directory.contains(disk, resolver.getImageId())) {
                FullHttpResponse resp = HttpUtil.newResponse(BAD_REQUEST,
                        "Image[id=" + resolver.getImageId() + "] already exist");
                return CompletableFuture.completedFuture(resp);
            }

            return store.save(
                    disk,
                    resolver.getImageId(),
                    resolver.getImageType(),
                    resolver.getImageData(),
                    resolver.getImageExpireTime()
            ).handle((image, ex) -> {
                if (ex != null) {
                    throw new UncheckedExecutionException(ex);
                }
                directory.save(disk, image.getIndex());
                return HttpUtil.newResponse(OK);
            });
        } catch (Exception e) {
            CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        } finally {
            decoder.destroy();
        }
    }

    private static class ImageStoreRequestResolver {

        private FullHttpResponse errorResponse;

        private long imageId;
        private Image.Type imageType;
        private long imageExpireTime;
        private byte[] imageData;

        public ImageStoreRequestResolver(HttpPostRequestDecoder decoder, Snowflake snowflake)
                throws IOException {
            
            InterfaceHttpData idData = decoder.getBodyHttpData("id");
            if (idData == null) {
                this.imageId = snowflake.nextId();
            } else if (!(idData instanceof Attribute)) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST, "Can't resolve image id.");
                return;
            } else {
                try {
                    this.imageId = Long.parseLong(((Attribute) idData).getValue());
                } catch (NumberFormatException e) {
                    this.errorResponse = HttpUtil.newResponse(
                            BAD_REQUEST,
                            "Image id " + ((Attribute) idData).getValue() + " is not long value.");
                    return;
                }
            }

            // read file
            InterfaceHttpData fileData = decoder.getBodyHttpData("file");
            if (fileData == null) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST, "No image file.");
                return;
            }
            if (!(fileData instanceof FileUpload)) {
                this.errorResponse = HttpUtil.newResponse(BAD_REQUEST,
                        "Not multipart/form-data request.");
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
                            "Expire time " + ((Attribute) expireTimeData).getValue()
                                    + " is not unix time stamp.");
                    return;
                }
            }
        }

        public boolean hasError() {
            return errorResponse != null;
        }

        public FullHttpResponse getErrorResponse() {
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
    }
}
