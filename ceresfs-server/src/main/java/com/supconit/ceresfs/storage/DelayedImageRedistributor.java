package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.http.HttpClientPool;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;
import com.supconit.ceresfs.topology.TopologyChangeListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;

@Component
public class DelayedImageRedistributor implements ImageRedistributor, TopologyChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedImageRedistributor.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final Topology topology;
    private final VolumeContainer volumeContainer;
    private final ImageDirectory imageDirectory;
    private final ImageStore imageStore;

    @Autowired
    public DelayedImageRedistributor(Topology topology,
                                     VolumeContainer volumeContainer,
                                     ImageDirectory imageDirectory,
                                     ImageStore imageStore) {
        this.topology = topology;
        this.volumeContainer = volumeContainer;
        this.imageDirectory = imageDirectory;
        this.imageStore = imageStore;
    }

    @Override
    public boolean isRunning() {
        return lock.getHoldCount() > 0;
    }

    @Override
    public void redistribute() {
        lock.lock();
        try {
            List<Disk> disks = topology.localNode().getDisks();

            for (Disk disk : disks) {
                List<Volume> volumes = volumeContainer.getAllVolumes(disk.getPath());
                if (volumes == null || volumes.isEmpty()) {
                    LOG.info("{} has no volumes, skipped", disk);
                    continue;
                }

                final Node localnode = topology.localNode();
                volumes.parallelStream().forEach(volume -> {
                    final ReentrantLock readLock = volume.getReadLock();
                    final Volume.Reader reader = volumeContainer.getReader(volume.getFile());
                    readLock.lock();
                    try {
                        reader.seek(0L);
                        Image image;
                        while ((image = reader.next()) != null) {
                            Image.Index index = image.getIndex();
                            long id = index.getId();
                            Disk route = topology.route(id);
                            Node node = route.getNode();
                            if (node.equals(localnode)) {
                                if (route.getId() != disk.getId()) {
                                    // TODO rewrite
                                }
                            } else {

                            }
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        readLock.unlock();
                    }
                });
            }
        } finally {
            lock.unlock();
        }
    }

    public void distribute(Node node, Image image) throws Exception {
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                "/image");
        HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, true);
        Image.Index index = image.getIndex();
        encoder.addBodyAttribute("id", String.valueOf(index.getId()));
        encoder.addBodyAttribute("expireTime", String.valueOf(index.getExpireTime()));

        HttpClientPool.getOrCreate(node.getHostAddress(), node.getPort())
                .newCall(request).whenComplete((res, ex) -> {
            // TODO
        });

    }

    @Override
    public void onNodeAdded(Node node) {
        
    }

    @Override
    public void onNodeRemoved(Node node) {

    }

    @Override
    public void onDiskAdded(Disk disk) {

    }

    @Override
    public void onDiskRemoved(Disk disk) {

    }

    @Override
    public void onDiskWeightChanged(Disk original, Disk present) {

    }
}
