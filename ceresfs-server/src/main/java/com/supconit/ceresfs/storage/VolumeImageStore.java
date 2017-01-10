package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.retry.OneTimeRetryStrategy;
import com.supconit.ceresfs.retry.RetryStrategy;
import com.supconit.ceresfs.retry.RetrySupplier;
import com.supconit.ceresfs.topology.Disk;

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
public class VolumeImageStore implements ImageStore, DisposableBean {

    private final VolumeContainer container;
    private final ExecutorService executor;

    @Autowired
    public VolumeImageStore(VolumeContainer volumeContainer) {
        this.container = volumeContainer;
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public Image get(Disk disk, Image.Index index) throws IOException {
        File volume = new File(disk.getPath(), String.valueOf(index.getVolume()));
        return container.getReader(volume).read(index.getOffset());
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data) {
        return save(disk, id, type, data, -1L, new OneTimeRetryStrategy(1000L));
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data,
                                         long expireTime) {
        return save(disk, id, type, data, expireTime, new OneTimeRetryStrategy(1000L));
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data,
                                         RetryStrategy retryStrategy) {
        return save(disk, id, type, data, -1L, new OneTimeRetryStrategy(1000L));
    }

    @Override
    public CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data,
                                         long expireTime, RetryStrategy retryStrategy) {
        return CompletableFuture.supplyAsync(
                new RetrySupplier<>(() -> {
                    try {
                        Image.Index index = new Image.Index();
                        index.setId(id);
                        index.setType(type);
                        index.setExpireTime(expireTime);
                        Image image = new Image(index, data);
                        container.getAppendableWriter(disk.getPath()).write(image);
                        return image;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }, retryStrategy)
                , executor);
    }

    @Override
    public void delete(Disk disk, Image.Index index) throws IOException {
        File volume = new File(disk.getPath(), String.valueOf(index.getVolume()));
        container.getUpdater(volume).markDeleted(index.getOffset());
    }

    @Override
    public void destroy() throws Exception {
        this.executor.shutdown();
    }
}
