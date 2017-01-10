package com.supconit.ceresfs.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;

import com.supconit.ceresfs.config.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class PooledVolumeContainer implements VolumeContainer {

    private static final Logger LOG = LoggerFactory.getLogger(PooledVolumeContainer.class);
    private final Cache<String, Volume.Writer> writerByPath = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .concurrencyLevel(4)
            .removalListener((RemovalListener<String, Volume.Writer>) notification -> {
                try {
                    LOG.debug("Close volume writer [{}] ", notification.getKey());
                    notification.getValue().close();
                } catch (IOException e) {
                    LOG.error("Close volume writer [" + notification.getKey() + "] error.", e);
                }
            })
            .build();
    private final Cache<String, Volume.Reader> readerByPath = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .concurrencyLevel(4)
            .removalListener((RemovalListener<String, Volume.Reader>) notification -> {
                try {
                    LOG.debug("Close volume reader [{}] ", notification.getKey());
                    notification.getValue().close();
                } catch (IOException e) {
                    LOG.error("Close volume reader [" + notification.getKey() + "] error.", e);
                }
            })
            .build();
    private final Cache<String, Volume.Updater> updaterByPath = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .concurrencyLevel(4)
            .removalListener((RemovalListener<String, Volume.Updater>) notification -> {
                try {
                    LOG.debug("Close volume updater [{}] ", notification.getKey());
                    notification.getValue().close();
                } catch (IOException e) {
                    LOG.error("Close volume updater [" + notification.getKey() + "] error.", e);
                }
            })
            .build();
    // 1 volume per disk
    // TODO allow multiple volumes?
    private final Map<String, Volume.Writer> appendableWriterByDisk = new HashMap<>();
    private final Configuration config;
    private Map<String, Volume> volumeByPath = new ConcurrentHashMap<>();

    @Autowired
    public PooledVolumeContainer(Configuration config) {
        this.config = config;
    }

    @Override
    public Volume getVolume(File volume) {
        Assert.notNull(volume);
        Assert.isTrue(volume.exists());
        return volumeByPath.getOrDefault(volume.getAbsolutePath(), new Volume(volume));
    }


    @Override
    public Volume.Writer getAppendableWriter(String disk) {
        Volume.Writer writer = appendableWriterByDisk.get(disk);
        if (writer == null || writer.length() >= config.getVolumeMaxSize()) {
            synchronized (appendableWriterByDisk) {
                try {
                    if (writer == null) {
                        writer = newVolumeWriter(disk);
                    } else if (writer.length() >= config.getVolumeMaxSize()) {
                        writer.close();
                        writer = newVolumeWriter(disk);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return writer;
    }

    private Volume.Writer newVolumeWriter(String diskPath) throws IOException {
        File file = new File(diskPath, String.valueOf(System.currentTimeMillis()));
        if (file.exists()) {
            return newVolumeWriter(diskPath);
        } else {
            if (!file.createNewFile()) {
                throw new IOException("Create new file " + file.getName() + " error");
            }
            Volume volume = new Volume(file);
            Volume.Writer writer = Volume.createWriter(volume);
            appendableWriterByDisk.put(diskPath, writer);
            return writer;
        }
    }

    @Override
    public Volume.Writer getWriter(File volume) {
        try {
            return writerByPath.get(volume.getAbsolutePath(), () ->
                    Volume.createWriter(getVolume(volume)));
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Volume.Reader getReader(File volume) {
        try {
            return readerByPath.get(volume.getAbsolutePath(), () ->
                    Volume.createReader(getVolume(volume)));
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Volume.Updater getUpdater(File volume) {
        try {
            return updaterByPath.get(volume.getAbsolutePath(), () ->
                    Volume.createUpdater(getVolume(volume)));
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public List<Volume> getAllVolumes(String disk) {
        File[] files = new File(disk).listFiles(p -> p.getName().matches("\\d*"));
        if (files == null || files.length == 0) {
            return null;
        }
        return Stream.of(files).map(this::getVolume).collect(Collectors.toList());
    }


    @Override
    public void disableVolume(File volume) {
        // close reader , writer and updater
        // assert volume is fully locked
        Assert.isTrue(volume.exists());
        volumeByPath.remove(volume.getAbsolutePath());
        disableWriter(volume);
        disableReader(volume);
        disableUpdater(volume);
    }

    @Override
    public void disableWriter(File volume) {
        String disk = volume.getParent();
        synchronized (appendableWriterByDisk) {
            Volume.Writer appendableWriter = appendableWriterByDisk.get(disk);
            if (appendableWriter != null) {
                appendableWriterByDisk.remove(disk);
                closeSilently(appendableWriter);
            }
        }

        String key = volume.getAbsolutePath();
        Volume.Writer writer = writerByPath.getIfPresent(key);
        if (writer != null) {
            writerByPath.invalidate(key);
        }
    }

    @Override
    public void disableReader(File volume) {
        String key = volume.getAbsolutePath();
        Volume.Reader reader = readerByPath.getIfPresent(key);
        if (reader != null) {
            readerByPath.invalidate(key);
        }
    }

    @Override
    public void disableUpdater(File volume) {
        String key = volume.getAbsolutePath();
        Volume.Updater updater = updaterByPath.getIfPresent(key);
        if (updater != null) {
            updaterByPath.invalidate(updater);
        }
    }

    @Override
    public void deleteVolume(File volume) {
        disableVolume(volume);
        boolean delete = volume.delete();
        if (!delete) {
            throw new UncheckedIOException(new IOException("File " + volume + " delete failed"));
        }
    }

    private void closeSilently(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            LOG.error("Close " + closeable + " error ", e);
        }
    }
}
