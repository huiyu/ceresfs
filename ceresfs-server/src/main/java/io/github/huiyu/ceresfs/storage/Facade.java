package io.github.huiyu.ceresfs.storage;

import com.google.common.util.concurrent.UncheckedExecutionException;

import io.github.huiyu.ceresfs.ImageType;
import io.github.huiyu.ceresfs.cache.Cache;
import io.github.huiyu.ceresfs.retry.RetryStrategy;
import io.github.huiyu.ceresfs.topology.Disk;
import io.github.huiyu.ceresfs.topology.Topology;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;

@Component
public class Facade {

    private final Topology topology;
    private final Cache cache;
    private final Directory directory;
    private final Store store;

    @Autowired
    public Facade(Topology topology, Cache cache, Directory directory, Store store) {
        this.cache = cache;
        this.topology = topology;
        this.directory = directory;
        this.store = store;
    }

    public Image get(Disk disk, long id) {
        Image image = cache.get(id);
        if (image != null) {
            return image;
        }

        ImageIndex index = directory.get(disk, id);
        if (index == null) {
            return null;
        }
        try {
            image = store.get(disk, index);
            if (image != null) {
                cache.put(image);
            }
            return image;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Image find(long id) {
        for (Disk disk : topology.getLocalNode().getDisks()) {
            Image image = get(disk, id);
            if (image != null) {
                return image;
            }
        }
        return null;
    }

    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data) {
        return store.save(disk, id, type, data)
                .handle((image, ex) -> afterImageSaved(disk, image, ex));

    }

    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data,
                                         long expireTime) {
        return store.save(disk, id, type, data, expireTime)
                .handle((image, ex) -> afterImageSaved(disk, image, ex));
    }

    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data,
                                         RetryStrategy retryStrategy) {
        return store.save(disk, id, type, data, retryStrategy)
                .handle((image, ex) -> afterImageSaved(disk, image, ex));
    }


    public CompletableFuture<Image> save(Disk disk, long id, ImageType type, byte[] data,
                                         long expireTime, RetryStrategy retryStrategy) {
        return store.save(disk, id, type, data, expireTime, retryStrategy)
                .handle((image, ex) -> afterImageSaved(disk, image, ex));
    }

    private Image afterImageSaved(Disk disk, Image image, Throwable ex) {
        if (ex != null)
            throw new UncheckedExecutionException(ex);
        else {
            directory.save(disk, image.getIndex());
            return image;
        }
    }

    public void delete(Disk disk, long id) {
        ImageIndex index = directory.get(disk, id);
        if (index != null) {
            try {
                store.delete(disk, index);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                cache.evict(id);
            }
        }
    }
}
