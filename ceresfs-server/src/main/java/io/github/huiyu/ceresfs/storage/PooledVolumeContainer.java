package io.github.huiyu.ceresfs.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.github.huiyu.ceresfs.config.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
public class PooledVolumeContainer implements VolumeContainer {

    // 5 minute rule
    private static final int EXPIRE_TIME = 5;
    private static final TimeUnit EXPIRE_TIME_UNIT = TimeUnit.SECONDS;

    private static final Logger LOG = LoggerFactory.getLogger(PooledVolumeContainer.class);

    protected CommonPool<Volume.Writer> writerPool;
    protected CommonPool<Volume.Reader> readerPool;
    protected ActiveWriterPool activeWriterPool;

    @Autowired
    public PooledVolumeContainer(Configuration config) {
        this.writerPool = new CommonPool<>(EXPIRE_TIME, EXPIRE_TIME_UNIT);
        this.readerPool = new CommonPool<>(EXPIRE_TIME, EXPIRE_TIME_UNIT);
        this.activeWriterPool = new ActiveWriterPool(config.getVolumeWriteParallelism(), config.getVolumeMaxSize());
    }

    private static void closeSilently(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            LOG.error("Close " + closeable + " error ", e);
        }
    }

    @Override
    public Volume.Writer getActiveWriter(String disk) {
        return activeWriterPool.select(disk);
    }

    @Override
    public Volume.Writer getWriter(File volume) {
        if (!volume.exists()) {
            return null;
        }
        Volume.Writer writer = activeWriterPool.get(volume);
        if (writer == null) {
            writer = writerPool.computeIfAbsent(volume.getAbsolutePath(),
                    () -> Volume.createWriter(volume));
        }
        return writer;
    }

    @Override
    public Volume.Reader getReader(File volume) {
        if (!volume.exists()) {
            return null;
        }
        return readerPool.computeIfAbsent(volume.getAbsolutePath(),
                () -> Volume.createReader(volume));
    }

    @Override
    public List<File> getAllVolumes(String disk) {
        File[] files = new File(disk).listFiles(p -> p.getName().matches("\\d*"));
        if (files == null || files.length == 0) {
            return null;
        }
        return Arrays.asList(files);
    }

    @Override
    public void closeVolume(File volume) {
        // close reader , writer and updater
        // assert volume is fully locked
        Assert.isTrue(volume.exists());
        closeWriter(volume);
        closeReader(volume);
    }

    @Override
    public void closeWriter(File volume) {
        activeWriterPool.disable(volume);
        writerPool.close(volume.getAbsolutePath());
    }

    @Override
    public void closeReader(File volume) {
        readerPool.close(volume.getAbsolutePath());
    }

    @Override
    public void deleteVolume(File volume) {
        closeVolume(volume);
        boolean delete = volume.delete();
        if (!delete) {
            throw new UncheckedIOException(new IOException("File " + volume + " delete failed"));
        }
    }

    /**
     * Common pool based on guava cache
     */
    public static final class CommonPool<E extends Closeable> implements Closeable {

        final Cache<String, E> cache;

        public CommonPool(long expireTime, TimeUnit expireTimeUnit) {
            this.cache = CacheBuilder.newBuilder()
                    .expireAfterAccess(expireTime, expireTimeUnit)
                    .concurrencyLevel(4)
                    .removalListener((RemovalListener<String, E>) notification -> {
                        try {
                            LOG.debug("Close {} ", notification.getKey());
                            notification.getValue().close();
                        } catch (IOException e) {
                            LOG.error("Close " + notification.getKey() + " error.", e);
                        }
                    })
                    .build();
        }

        public E computeIfAbsent(String key, Callable<E> valueLoader) {
            try {
                return cache.get(key, valueLoader);
            } catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            }
        }

        public void close(String key) {
            cache.invalidate(key);
        }

        @Override
        public void close() throws IOException {
            cache.invalidateAll();
        }
    }

    /**
     * Active writer pool
     */
    public static final class ActiveWriterPool implements Closeable {

        final int size;
        final long maxVolumeSize;
        final Map<String, Volume.Writer[]> writersByDisk;
        final Random random = new Random(47);

        public ActiveWriterPool(int size, long maxVolumeSize) {
            this.size = size;
            this.maxVolumeSize = maxVolumeSize;
            this.writersByDisk = new HashMap<>();
        }

        public synchronized Volume.Writer select(String disk) {
            Volume.Writer[] writers = writersByDisk.get(disk);
            if (writers == null) {
                writers = new Volume.Writer[size];
                for (int i = 0; i < size; i++) {
                    Volume.Writer writer = newWriter(disk);
                    writers[i] = writer;
                }
                writersByDisk.put(disk, writers);
            }

            int next = random.nextInt(size);
            Volume.Writer writer = writers[next];
            if (writer.length() > maxVolumeSize) {
                writer = newWriter(disk);
                writers[next] = writer;
            }
            return writer;
        }

        public Volume.Writer get(File volume) {
            String disk = volume.getParent();
            Volume.Writer[] writers = writersByDisk.get(disk);

            if (writers == null || writers.length == 0) {
                return null;
            }

            for (Volume.Writer writer : writers) {
                if (writer.getVolume().equals(volume)) {
                    return writer;
                }
            }
            return null;
        }

        public synchronized void disable(File volume) {
            String disk = volume.getParent();

            Volume.Writer[] writers = writersByDisk.get(disk);
            if (writers == null)
                return;

            for (int i = 0; i < writers.length; i++) {
                Volume.Writer w = writers[i];
                if (w.getVolume().equals(volume)) {
                    closeSilently(w);
                    w = newWriter(disk);
                    writers[i] = w;
                    break;
                }
            }
        }

        private Volume.Writer newWriter(String disk) {
            try {
                File file = new File(disk, String.valueOf(System.currentTimeMillis()));
                if (file.exists()) {
                    return newWriter(disk);
                } else {
                    if (!file.createNewFile()) {
                        throw new IOException("Create new file " + file.getName() + " error");
                    }
                    Volume.Writer writer = Volume.createWriter(file);
                    return writer;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            for (Volume.Writer[] writers : writersByDisk.values()) {
                for (Volume.Writer writer : writers) {
                    closeSilently(writer);
                }
            }
        }
    }
}
