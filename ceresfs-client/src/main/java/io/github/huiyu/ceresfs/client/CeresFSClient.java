package io.github.huiyu.ceresfs.client;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.github.huiyu.ceresfs.Const;
import io.github.huiyu.ceresfs.ImageType;
import io.github.huiyu.ceresfs.http.HttpBadResponseException;
import io.github.huiyu.ceresfs.http.HttpClientPool;
import io.github.huiyu.ceresfs.retry.NTimesRetryStrategy;
import io.github.huiyu.ceresfs.retry.RetryStrategy;
import io.github.huiyu.ceresfs.retry.RetrySupplier;
import io.github.huiyu.ceresfs.topology.Disk;
import io.github.huiyu.ceresfs.topology.ListenableRouter;
import io.github.huiyu.ceresfs.topology.Node;
import io.github.huiyu.ceresfs.util.HttpUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;

import static io.github.huiyu.ceresfs.Const.HTTP_HEADER_CONTENT_TYPE;
import static io.github.huiyu.ceresfs.Const.HTTP_HEADER_EXPIRE_TIME;
import static io.github.huiyu.ceresfs.Const.HTTP_HEADER_IMAGE_ID;

public class CeresFSClient implements Closeable {

    private final Random random = new Random();
    private final CuratorFramework client;
    private final ListenableRouter router;
    private final HttpClientPool httpClientPool;

    private final int replication;
    private final int vnodeFactor;
    private final RetryStrategy retryStrategy;

    public CeresFSClient(String zookeeperAddress) throws Exception {
        this(zookeeperAddress, new NTimesRetryStrategy(6, 10000L));
    }

    public CeresFSClient(String zookeeperAddress, RetryStrategy retryStrategy) throws Exception {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        client = CuratorFrameworkFactory.newClient(zookeeperAddress, retryPolicy);
        client.start();

        byte[] bytes = client.getData().forPath(Const.ZK_CONFIG_PATH);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        replication = buffer.get();
        vnodeFactor = buffer.getInt();

        router = new ListenableRouter(client, vnodeFactor);
        httpClientPool = new HttpClientPool();

        this.retryStrategy = retryStrategy;
    }

    public CompletableFuture<Image> get(long id) {
        return CompletableFuture.supplyAsync(new RetrySupplier<>(() -> {
            try {
                Disk disk = router.route(Longs.toByteArray(id));
                Node node = disk.getNode();
                FullHttpRequest req = HttpUtil.newRequest(HttpMethod.GET, "/image?id=" + id);
                FullHttpResponse resp = httpClientPool
                        .getOrCreate(node.getHostAddress(), node.getPort())
                        .newCall(req)
                        .get();
                if (!resp.status().equals(HttpResponseStatus.OK))
                    throw new HttpBadResponseException(resp);
                return new Image(
                        id,
                        ImageType.fromMimeType(resp.headers().get(HTTP_HEADER_CONTENT_TYPE)),
                        Long.parseLong(resp.headers().get(HTTP_HEADER_EXPIRE_TIME)),
                        resp.content().array());
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        }, retryStrategy));
    }

    public CompletableFuture<Image> save(long id, ImageType type, byte[] data) {
        return save(id, type, -1L, data);
    }

    public CompletableFuture<Image> save(long id, ImageType type, long expireTime, byte[] data) {
        return save(id, type, replication, expireTime, data);
    }

    public CompletableFuture<Image> save(long id, ImageType type, int replication, long expireTime,
                                         byte[] data) {
        return CompletableFuture.supplyAsync(new RetrySupplier<>(() -> {
            try {
                FullHttpRequest request =
                        HttpUtil.newImageUploadRequest(id, type, replication, expireTime, data);
                Disk disk = router.route(Longs.toByteArray(id));
                Node node = disk.getNode();
                FullHttpResponse resp = httpClientPool
                        .getOrCreate(node.getHostAddress(), node.getPort())
                        .newCall(request)
                        .get();
                if (!resp.status().equals(HttpResponseStatus.OK))
                    throw new HttpBadResponseException(resp);
                return new Image(id, type, expireTime, data);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        }, retryStrategy));
    }

    public CompletableFuture<Image> save(ImageType type, byte[] data) {
        return save(type, -1L, data);
    }

    public CompletableFuture<Image> save(ImageType type, long expireTime, byte[] data) {
        return save(type, replication, expireTime, data);
    }

    public CompletableFuture<Image> save(ImageType type, int replication, long expireTime,
                                         byte[] data) {
        return CompletableFuture.supplyAsync(new RetrySupplier<>(() -> {
            try {
                FullHttpRequest request =
                        HttpUtil.newImageUploadRequest(type, replication, expireTime, data);
                request.headers().set(Const.HTTP_HEADER_MAX_FORWARDS, 1);
                Disk disk = router.route(Longs.toByteArray(random.nextLong()));
                Node node = disk.getNode();
                FullHttpResponse resp = httpClientPool
                        .getOrCreate(node.getHostAddress(), node.getPort())
                        .newCall(request)
                        .get();
                if (!resp.status().equals(HttpResponseStatus.OK)) {
                    throw new HttpBadResponseException(resp);
                }
                long id = Long.parseLong(resp.headers().get(HTTP_HEADER_IMAGE_ID));
                return new Image(id, type, expireTime, data);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        }, retryStrategy));
    }

    public CompletableFuture<Void> renewExpireTime(long id, long expireTime) {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> delete(long id) {
        return CompletableFuture.supplyAsync(new RetrySupplier<>(() -> {
            try {
                FullHttpRequest request = HttpUtil.newRequest(HttpMethod.DELETE, "/image");
                HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, true);
                encoder.addBodyAttribute("id", String.valueOf(id));
                encoder.finalizeRequest();

                Node node = router.route(Longs.toByteArray(id)).getNode();
                FullHttpResponse resp = httpClientPool
                        .getOrCreate(node.getHostAddress(), node.getPort())
                        .newCall(request)
                        .get();

                if (!resp.status().equals(HttpResponseStatus.OK))
                    throw new HttpBadResponseException(resp);
                return null;
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        }, retryStrategy));
    }

    @Override
    public void close() throws IOException {
        router.close();
        httpClientPool.close();
        client.close();
    }
}