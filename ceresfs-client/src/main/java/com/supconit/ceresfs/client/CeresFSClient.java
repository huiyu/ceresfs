package com.supconit.ceresfs.client;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.UncheckedExecutionException;

import com.supconit.ceresfs.Const;
import com.supconit.ceresfs.ImageType;
import com.supconit.ceresfs.http.HttpBadResponseException;
import com.supconit.ceresfs.http.HttpClientPool;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.ListenableRouter;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.util.HttpUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;

import static com.supconit.ceresfs.Const.HTTP_HEADER_CONTENT_TYPE;
import static com.supconit.ceresfs.Const.HTTP_HEADER_EXPIRE_TIME;
import static com.supconit.ceresfs.Const.HTTP_HEADER_IMAGE_ID;

public class CeresFSClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CeresFSClient.class);


    private final Random random = new Random();
    private final CuratorFramework client;
    private final ListenableRouter router;
    private final HttpClientPool httpClientPool;

    private final int vnodeFactor;

    public CeresFSClient(String zookeeperAddress) throws Exception {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        client = CuratorFrameworkFactory.newClient(zookeeperAddress, retryPolicy);
        client.start();

        byte[] bytes = client.getData().forPath(Const.ZK_CONFIG_PATH);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.get(); // replication
        vnodeFactor = buffer.getInt();

        router = new ListenableRouter(client, vnodeFactor);
        httpClientPool = new HttpClientPool();
    }

    public CompletableFuture<Image> get(long id) {
        Disk disk = router.route(Longs.toByteArray(id));
        Node node = disk.getNode();
        FullHttpRequest req = HttpUtil.newRequest(HttpMethod.GET, "/image?id=" + id);
        return httpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                .newCall(req)
                .handleAsync((res, ex) -> {
                    if (ex != null)
                        throw new UncheckedExecutionException(ex);
                    if (!res.status().equals(HttpResponseStatus.OK))
                        throw new HttpBadResponseException(res);
                    return new Image(
                            id,
                            ImageType.fromMimeType(res.headers().get(HTTP_HEADER_CONTENT_TYPE)),
                            Long.parseLong(res.headers().get(HTTP_HEADER_EXPIRE_TIME)),
                            res.content().array());
                });
    }

    public CompletableFuture<Image> save(long id, ImageType type, long expireTime, byte[] data) {
        try {
            FullHttpRequest request = HttpUtil.newImageUploadRequest(id, type, expireTime, data);
            Disk disk = router.route(Longs.toByteArray(id));
            Node node = disk.getNode();
            return httpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                    .newCall(request)
                    .handleAsync((res, ex) -> {
                        if (ex != null)
                            throw new UncheckedExecutionException(ex);
                        if (!res.status().equals(HttpResponseStatus.OK))
                            throw new HttpBadResponseException(res);
                        return new Image(id, type, expireTime, data);
                    });
        } catch (Exception e) {
            CompletableFuture<Image> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    public CompletableFuture<Image> save(long id, ImageType type, byte[] data) {
        return save(id, type, -1L, data);
    }

    public CompletableFuture<Image> save(ImageType type, long expireTime, byte[] data) {
        try {
            FullHttpRequest request = HttpUtil.newImageUploadRequest(type, expireTime, data);
            request.headers().set(Const.HTTP_HEADER_MAX_FORWARDS, 1);
            Disk disk = router.route(Longs.toByteArray(random.nextLong()));
            Node node = disk.getNode();
            return httpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                    .newCall(request)
                    .handleAsync((res, ex) -> {
                        if (ex != null)
                            throw new UncheckedExecutionException(ex);
                        if (!res.status().equals(HttpResponseStatus.OK))
                            throw new HttpBadResponseException(res);

                        long id = Long.parseLong(res.headers().get(HTTP_HEADER_IMAGE_ID));
                        return new Image(id, type, expireTime, data);
                    });
        } catch (Exception e) {
            CompletableFuture<Image> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    public CompletableFuture<Image> save(ImageType type, byte[] data) {
        return save(type, -1L, data);
    }

    public CompletableFuture<Void> renewExpireTime(long id, long expireTime) {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> delete(long id) {
        try {
            FullHttpRequest request = HttpUtil.newRequest(HttpMethod.DELETE, "/image");
            HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, true);
            encoder.addBodyAttribute("id", String.valueOf(id));
            encoder.finalizeRequest();
            Node node = router.route(Longs.toByteArray(id)).getNode();
            return httpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                    .newCall(request)
                    .handleAsync((res, ex) -> {
                        if (ex != null)
                            throw new UncheckedExecutionException(ex);
                        if (!res.status().equals(HttpResponseStatus.OK))
                            throw new HttpBadResponseException(res);
                        return null;
                    });
        } catch (Exception e) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public void close() throws IOException {
        router.close();
        httpClientPool.close();
        client.close();
    }

    public static void main(String[] args) throws Exception {
        CeresFSClient client = new CeresFSClient("127.0.0.1:2181");
        FileInputStream in = new FileInputStream("/Users/yuhui/Pictures/saber.jpg");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteStreams.copy(in, out);
        in.close();

        byte[] data = out.toByteArray();
        for (int i = 0; i < 100000; i++) {
            client.save(ImageType.JPG, data)
                    .whenComplete((image, ex) -> {
                        if (ex != null) {
                            ex.printStackTrace();
                        } else {
                            System.out.println(image.getId());
                        }
                    });
            Thread.sleep(500L);
        }
    }
}