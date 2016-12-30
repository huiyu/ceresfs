package com.supconit.ceresfs;

import com.supconit.ceresfs.topology.Disk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties("ceresfs")
public class DefaultConfiguration implements Configuration, InitializingBean, DisposableBean {

    private CuratorFramework zookeeperClient;

    private short id;
    private int port;
    private String mode;
    private String zookeeperQuorum;
    private double diskDefaultWeight;
    private int vnodeFactor;
    private long volumeLimit;
    private int imageMaxSize;
    private List<Disk> disks;

    @Override
    public CuratorFramework getZookeeperClient() {
        return zookeeperClient;
    }

    @Override
    public List<Disk> getDisks() {
        return disks;
    }

    public void setDisks(List<Disk> disks) {
        this.disks = disks;
    }

    @Override
    public short getId() {
        return id;
    }

    public void setId(short id) {
        this.id = id;
    }

    @Override
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    @Override
    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    @Override
    public double getDiskDefaultWeight() {
        return diskDefaultWeight;
    }

    public void setDiskDefaultWeight(double diskDefaultWeight) {
        this.diskDefaultWeight = diskDefaultWeight;
    }

    @Override
    public int getVnodeFactor() {
        return vnodeFactor;
    }

    public void setVnodeFactor(int vnodeFactor) {
        this.vnodeFactor = vnodeFactor;
    }

    @Override
    public long getVolumeLimit() {
        return volumeLimit;
    }

    public void setVolumeLimit(long volumeLimit) {
        this.volumeLimit = volumeLimit;
    }

    @Override
    public int getImageMaxSize() {
        return imageMaxSize;
    }

    public void setImageMaxSize(int imageMaxSize) {
        this.imageMaxSize = imageMaxSize;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initZookeeperClient();
    }

    @Override
    public void destroy() throws Exception {
        this.zookeeperClient.close();
    }

    private void initZookeeperClient() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        zookeeperClient = CuratorFrameworkFactory.newClient(getZookeeperQuorum(), retryPolicy);
        zookeeperClient.start();
    }
}

