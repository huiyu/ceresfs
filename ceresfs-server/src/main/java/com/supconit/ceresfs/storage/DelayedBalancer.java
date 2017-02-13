package com.supconit.ceresfs.storage;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.UncheckedExecutionException;

import com.supconit.ceresfs.Const;
import com.supconit.ceresfs.http.HttpClientPool;
import com.supconit.ceresfs.retry.NTimesRetryStrategy;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;

public class DelayedBalancer implements Balancer {

    enum State {
        STOPPED, RUNNING, CANCELLED
    }

    private static final Logger LOG = LoggerFactory.getLogger(DelayedBalancer.class);

    private final Topology topology;
    private final HttpClientPool httpClientPool;
    private final Directory directory;
    private final Store store;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition cancelled = lock.newCondition();
    private final Condition stopped = lock.newCondition();
    private volatile State state = State.STOPPED;

    public DelayedBalancer(Topology topology,
                           HttpClientPool httpClientPool,
                           Directory directory,
                           Store store
    ) {
        this.topology = topology;
        this.httpClientPool = httpClientPool;
        this.directory = directory;
        this.store = store;
    }

    @Override
    public boolean isRunning() {
        return state.equals(State.RUNNING);
    }

    @Override
    public CompletableFuture<Void> start(long delay, TimeUnit delayTimeUnit) {
        if (state.equals(State.RUNNING)) {
            throw new IllegalStateException("Balancer is running");
        }

        if (state.equals(State.CANCELLED)) {
            waitForStop();
        }

        state = State.RUNNING;
        return CompletableFuture.runAsync(() -> {
            Stopwatch stopwatch = Stopwatch.createStarted();
            lock.lock();
            try {
                LOG.info("Balancer prepare for start...");
                cancelled.await(delay, delayTimeUnit);
                if (state.equals(State.CANCELLED)) {
                    return;
                }
                LOG.info("Balancer started...");
                run();
            } catch (InterruptedException e) {
                throw new UncheckedExecutionException(e);
            } finally {
                state = State.STOPPED;
                stopped.signalAll();
                lock.unlock();

                stopwatch.stop();
                long elapsed = stopwatch.elapsed(TimeUnit.SECONDS);
                LOG.info("Balancer stopped, cost {} seconds", elapsed);
            }
        });
    }

    @Override
    public void cancel() {
        if (state.equals(State.RUNNING)) {
            LOG.info("Cancelling balancer...");
            state = State.CANCELLED;
            if (lock.tryLock()) {
                try {
                    cancelled.signal();
                } finally {
                    lock.unlock();
                }
            }
        }
        waitForStop();
    }

    private void waitForStop() {
        lock.lock();
        try {
            while (state.equals(State.CANCELLED)) {
                stopped.awaitUninterruptibly();
                LOG.info("Balancer cancelled");
            }
        } finally {
            lock.unlock();
        }
    }

    protected void run() {
        Node localNode = topology.getLocalNode();
        List<Disk> disks = localNode.getDisks();
        disks.forEach(disk -> directory.forEachId(disk, id -> {
            // interrupted
            if (!isRunning()) {
                throw new UncheckedExecutionException(new InterruptedException());
            }
            Disk route = topology.route(id);
            if (route.getNode().getId() != localNode.getId()) { // not local 
                toRemote(route, getImageById(disk, id));
            } else if (route.getId() != disk.getId()) { // local but in another disk
                toLocal(route, getImageById(disk, id));
            }
            // ignore others
        }));
    }

    protected Image getImageById(Disk disk, long id) {
        try {
            ImageIndex index = directory.get(disk, id);
            Assert.notNull(index);
            Image image = store.get(disk, index);
            return image;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void toLocal(Disk disk, Image image) {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} move to {}", image.toString(), disk.toString());
            }
            ImageIndex index = image.getIndex();
            store.save(
                    disk,
                    image.getIndex().getId(),
                    image.getIndex().getType(),
                    image.getData(),
                    index.getExpireTime(),
                    new NTimesRetryStrategy(5, 100L)
            ).whenComplete((img, ex) -> {
                if (ex != null) {
                    // FIXME: roughly interrupt
                    LOG.error("Save to " + disk.toString() + " error", ex);
                    cancel();
                    return;
                }

                try {
                    store.delete(disk, index);
                    directory.delete(disk, index.getId());
                } catch (Exception e) {
                    // FIXME: roughly interrupt
                    LOG.error("Delete image " + image.toString() + " error", e);
                    cancel();
                }
            });
        } catch (Exception e) {
            LOG.error("Image distribute to " + disk.getNode().toString() + " error", e);
            throw new UncheckedExecutionException(e);
        }
    }

    protected void toRemote(Disk disk, Image image) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} move to {}", image.toString(), disk.toString());
        }

        Node node = disk.getNode();
        ImageIndex index = image.getIndex();
        FullHttpRequest request = newPostRequest(image);
        httpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                .newCall(request)
                .whenComplete((resp, ex) -> {
                    if (ex != null) {
                        // FIXME: roughly interrupt
                        LOG.error("Redistribute to " + node.toString() + " error", ex);
                        cancel();
                        return;
                    }

                    if (resp.status().equals(HttpResponseStatus.OK)) {
                        try {
                            store.delete(disk, index);
                            directory.delete(disk, index.getId());
                        } catch (Exception e) {
                            // FIXME: roughly interrupt
                            LOG.error("Delete file {} failed \n {}", node, resp.toString());
                            cancel();
                        }
                    } else {
                        // FIXME: roughly interrupt
                        LOG.error("Redistribute to {} failed \n {}", node, resp.toString());
                        cancel();
                    }
                });
    }

    protected FullHttpRequest newPostRequest(Image image) {
        try {
            ImageIndex index = image.getIndex();
            FullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.POST, "/image");
            HttpDataFactory factory = new DefaultHttpDataFactory();
            HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, true);
            encoder.addBodyAttribute(Const.HTTP_HEADER_IMAGE_ID, String.valueOf(index.getId()));
            encoder.addBodyAttribute(Const.HTTP_HEADER_EXPIRE_TIME, String.valueOf(index.getExpireTime()));

            FileUpload fileUpload = factory.createFileUpload(
                    request,
                    "file",
                    "test." + index.getType().getFileSuffix(),
                    "application/octet-stream",
                    "binary",
                    null,
                    image.getData().length);

            fileUpload.setContent(Unpooled.wrappedBuffer(image.getData()));
            encoder.addBodyHttpData(fileUpload);
            encoder.finalizeRequest();
            HttpContent content;
            while ((content = encoder.readChunk((ByteBufAllocator) null)) != null) {
                request.content().writeBytes(content.content());
            }
            return request;
        } catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

}
