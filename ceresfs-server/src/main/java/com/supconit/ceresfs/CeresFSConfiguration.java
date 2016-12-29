package com.supconit.ceresfs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;

@SpringBootConfiguration
public class CeresFSConfiguration {

    @Value("${ceresfs.port:9900}")
    private int port;
    @Value("${ceresfs.zookeeper.quorum}")
    private String zookeeperQuorum;
    @Value("${ceresfs.disks}")
    private String disks;
    @Value("${ceresfs.disk.defaultWeight:1.0}")
    private double diskDefaultWeight;
    @Value("${ceresfs.vnode.factor:10000}")
    private int vnodeFactor;
    @Value("${ceresfs.mode")
    private String mode;
    @Value("${ceresfs.volume.limit:17179869184}")
    private long volumeLimit; // default 16GB
    @Value("${ceresfs.image.maxSize:10485760}")
    private int imageMaxSize;

    public int getPort() {
        return port;
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public String getDisks() {
        return disks;
    }

    public double getDiskDefaultWeight() {
        return diskDefaultWeight;
    }

    public int getVnodeFactor() {
        return vnodeFactor;
    }

    public String getMode() {
        return mode;
    }

    public long getVolumeLimit() {
        return volumeLimit;
    }

    public int getImageMaxSize() {
        return imageMaxSize;
    }
}

