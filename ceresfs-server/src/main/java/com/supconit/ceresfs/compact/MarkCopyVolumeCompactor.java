package com.supconit.ceresfs.compact;

import com.supconit.ceresfs.config.Configuration;
import com.supconit.ceresfs.retry.NTimesRetryStrategy;
import com.supconit.ceresfs.storage.Image;
import com.supconit.ceresfs.storage.Directory;
import com.supconit.ceresfs.storage.Store;
import com.supconit.ceresfs.storage.Volume;
import com.supconit.ceresfs.storage.VolumeContainer;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class MarkCopyVolumeCompactor implements VolumeCompactor, InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(MarkCopyVolumeCompactor.class);

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private final Configuration config;
    private final Topology topology;
    private final VolumeContainer volumeContainer;
    private final Directory directory;
    private final Store store;

    private final ReentrantLock lock = new ReentrantLock();

    @Autowired
    public MarkCopyVolumeCompactor(Configuration config,
                                   Topology topology,
                                   VolumeContainer volumeContainer,
                                   Directory directory,
                                   Store store) {
        this.config = config;
        this.topology = topology;
        this.volumeContainer = volumeContainer;
        this.directory = directory;
        this.store = store;
    }

    public void compact() {
        lock.lock();
        try {
            final long currentTime = System.currentTimeMillis();
            List<Disk> disks = topology.getLocalNode().getDisks();
            for (Disk disk : disks) {
                LOG.info("Scanning {}", disk);
                List<File> volumes = volumeContainer.getAllVolumes(disk.getPath());
                if (volumes == null || volumes.isEmpty()) {
                    LOG.info("{} has no volumes, skipped.", disk);
                    continue;
                }

                volumes.parallelStream().forEach(volume -> {
                    // count dead space
                    long dead = mark(currentTime, disk, volume);
                    // do compact
                    if (dead > config.getVolumeMaxSize() * (1.0 - config.getVolumeCompactThreshold())) {
                        LOG.info("Volume {} has {} dead space, start compacting...",
                                volume.getName(), dead);
                        compact(currentTime, disk, volume);
                        LOG.info("Volume {} compacting completed",
                                volume.getName(), dead);
                    } else {
                        LOG.info("Volume {} has {} dead space, skipped",
                                volume.getName(), dead);
                    }
                });
            }
        } finally {
            lock.unlock();
        }
    }

    protected long mark(long currentTime, Disk disk, File volume) {
        final Volume.Reader reader = volumeContainer.getReader(volume);
        final ReentrantLock readLock = reader.getLock();
        readLock.lock();
        long invalid = 0;
        try {
            reader.seek(0L);
            Image image;
            while ((image = reader.next()) != null) {
                Image.Index index = image.getIndex();

                if (isDeleted(index)) {
                    invalid = invalid + Image.Index.FIXED_LENGTH + index.getSize();
                }
                // delete index of expired image
                if (isExpired(currentTime, index)) {
                    invalid = invalid + Image.Index.FIXED_LENGTH + index.getSize();
                    directory.delete(disk, index.getId());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            readLock.unlock();
        }
        return invalid;
    }

    protected void compact(long currentTime, Disk disk, File volume) {
        final Volume.Reader reader = volumeContainer.getReader(volume);
        final Volume.Writer writer = volumeContainer.getWriter(volume);
        final ReentrantLock writeLock = writer.getLock();
        writeLock.lock();
        try {
            // disable write first
            volumeContainer.closeWriter(volume);
            reader.seek(0L);
            Image image;
            while ((image = reader.next()) != null) {
                Image.Index index = image.getIndex();
                if (!isDeleted(index) && !isExpired(currentTime, index)) {
                    store.save(
                            disk,
                            index.getId(),
                            index.getType(),
                            image.getData(),
                            index.getExpireTime(),
                            new NTimesRetryStrategy(5, 1000L)
                    ).whenComplete((i, e) -> {
                        if (e != null) {
                            LOG.error("Redistribute error", e);
                        } else {
                            directory.save(disk, i.getIndex());
                        }
                    });
                }
            }
            // disable & delete volume after all
            volumeContainer.deleteVolume(volume);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            writeLock.unlock();
        }
    }

    protected boolean isExpired(long currentTime, Image.Index index) {
        return index.getExpireTime() > 0 && index.getExpireTime() < currentTime;
    }

    protected boolean isDeleted(Image.Index index) {
        return index.getFlag() == Image.FLAG_DELETED;
    }

    @Override
    public void destroy() throws Exception {
        executor.shutdown();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        executor.scheduleWithFixedDelay(this::compact,
                0L,
                config.getVolumeCompactPeriod(),
                config.getVolumeCompactPeriodTimeUnit());
    }

    @Override
    public boolean isRunning() {
        return lock.getHoldCount() > 0;
    }
}
