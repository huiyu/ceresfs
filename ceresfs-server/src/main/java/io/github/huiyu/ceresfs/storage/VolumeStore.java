package io.github.huiyu.ceresfs.storage;

import io.github.huiyu.ceresfs.ImageType;
import io.github.huiyu.ceresfs.retry.OneTimeRetryStrategy;
import io.github.huiyu.ceresfs.retry.RetryStrategy;
import io.github.huiyu.ceresfs.retry.RetrySupplier;
import io.github.huiyu.ceresfs.topology.Disk;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class VolumeStore implements Store, DisposableBean {

    private final VolumeContainer container;
    private final ExecutorService executor;

    @Autowired
    public VolumeStore(VolumeContainer volumeContainer) {
        this.container = volumeContainer;
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public Image get(Disk disk, ImageIndex index) throws IOException {
        File volume = new File(disk.getPath(), String.valueOf(index.getVolume()));
        return container.getReader(volume).read(index.getOffset());
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data) {
        return save(disk, id, type, data, -1L, new OneTimeRetryStrategy(1000L));
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data,
                                         long expireTime) {
        return save(disk, id, type, data, expireTime, new OneTimeRetryStrategy(1000L));
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data,
                                         RetryStrategy retryStrategy) {
        return save(disk, id, type, data, -1L, new OneTimeRetryStrategy(1000L));
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data,
                                         long expireTime, RetryStrategy retryStrategy) {
        return CompletableFuture.supplyAsync(
                new RetrySupplier<>(() -> {
                    try {
                        ImageIndex index = new ImageIndex();
                        index.setId(id);
                        index.setType(type);
                        index.setExpireTime(expireTime);
                        Image image = new Image(index, data);
                        container.getActiveWriter(disk.getPath()).write(image);
                        return image;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }, retryStrategy)
                , executor);
    }

    @Override
    public void delete(Disk disk, ImageIndex index) throws IOException {
        File volume = new File(disk.getPath(), String.valueOf(index.getVolume()));
        container.getWriter(volume).markDeleted(index.getOffset());
    }

    @Override
    public void destroy() throws Exception {
        this.executor.shutdown();
    }
}
