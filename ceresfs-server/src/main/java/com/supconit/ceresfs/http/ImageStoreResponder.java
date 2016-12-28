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
    private final ImageDirectory directory;
    private final ImageStore store;

    @Autowired
    public ImageStoreResponder(Topology topology, ImageDirectory directory, ImageStore store) {
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
        return new HttpMethod[]{HttpMethod.POST};
    }

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(USE_MEMORY, request);
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
                HttpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                        .newCall(request.copy())
                        .onSuccess(r -> ctx.writeAndFlush(r.copy()))
                        .onError(e -> ctx.writeAndFlush(
                                HttpUtil.newResponse(INTERNAL_SERVER_ERROR, e.getMessage())))
                        .execute();
                return;
            }

            store.save(disk, resolver.getImageId(), resolver.getImageType(), resolver.getImageData())
                    .setTime(resolver.getImageTime())
                    .setExpireTime(resolver.getImageExpireTime())
                    .onSuccess(image -> directory.save(disk, image.getIndex()))
                    .onError(error -> {
                        // TODO 
                    })
                    .execute(false);
            ctx.writeAndFlush(HttpUtil.newResponse(OK, OK.reasonPhrase()));
        } finally {
            decoder.destroy();
        }
    }

    private static class ImageStoreRequestResolver {

        private HttpResponse errorResponse;

        private long imageId;
        private Image.Type imageType;
        private long imageExpireTime;
        private long imageTime;
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

            // read time
            InterfaceHttpData timeData = decoder.getBodyHttpData("time");
            if (timeData == null) {
                // default is current time
                imageTime = System.currentTimeMillis();
            } else {
                try {
                    String timeValue = ((Attribute) timeData).getValue();
                    imageTime = Long.valueOf(timeValue);
                } catch (NumberFormatException e) {
                    this.errorResponse = HttpUtil.newResponse(
                            BAD_REQUEST,
                            "Time " + ((Attribute) timeData).getValue() + " is not unix time stamp.");
                    return;
                }

            }

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

        public long getImageTime() {
            return imageTime;
        }
    }
}
