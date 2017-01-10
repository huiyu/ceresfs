package com.supconit.ceresfs.config;

import com.supconit.ceresfs.CeresFS;
import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.util.Codec;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

@org.springframework.context.annotation.Configuration
public class ConfigurationService implements Configuration, InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationService.class);
    private static final String ZK_BASE_PATH = "/ceresfs";
    private static final String ZK_CONF_PATH = ZK_BASE_PATH + "/configuration";

    private volatile GlobalConfig globalConfig;

    private CuratorFramework zookeeperClient;
    private NodeCache globalConfigWatcher;

    @Autowired
    private LocalConfig localConfig;

    @Override
    public short getId() {
        return localConfig.getId();
    }

    @Override
    public int getPort() {
        return localConfig.getPort();
    }

    @Override
    public double getDiskDefaultWeight() {
        return localConfig.getDiskDefaultWeight();
    }

    @Override
    public long getVolumeMaxSize() {
        return localConfig.getVolumeMaxSize();
    }

    @Override
    public double getVolumeCompactThreshold() {
        return localConfig.getVolumeCompactThreshold();
    }

    @Override
    public long getVolumeCompactPeriod() {
        return localConfig.getVolumeCompactPeriod();
    }

    @Override
    public TimeUnit getVolumeCompactPeriodTimeUnit() {
        String timeUnit = localConfig.getVolumeCompactPeriodTimeunit();
        return TimeUnit.valueOf(timeUnit.toUpperCase());
    }

    @Override
    public int getImageMaxSize() {
        return localConfig.getImageMaxSize();
    }

    @Override
    public int getVnodeFactor() {
        return globalConfig.getVnodeFactor();
    }

    @Override
    public List<Disk> getDisks() {
        return localConfig.getDisks();
    }

    @Override
    public String getZookeeperAddress() {
        return localConfig.getZookeeperAddress();
    }

    @Override
    public CuratorFramework getZookeeperClient() {
        return zookeeperClient;
    }

    @Override
    public byte getReplication() {
        return globalConfig.getReplication();
    }

    @Override
    public void setReplication(byte replication) throws Exception {
        GlobalConfig globalConfig = this.globalConfig;
        globalConfig.setReplication(replication);
        checkAndWriteGlobalConfig(globalConfig);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // create and start zookeeper client
        createZookeeperClient();

        // write global config if not exist
        writeGlobalConfigIfNotExist();

        // watch global configuration
        watchGlobalConfig();

        // check local configurations
        checkLocalConfig(this.localConfig);

        // check and create disk directories
        createDisksIfNotExist();
    }

    private void createZookeeperClient() throws Exception {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        zookeeperClient = CuratorFrameworkFactory.newClient(
                localConfig.getZookeeperAddress(), retryPolicy);
        zookeeperClient.start();

        // create base zookeeper path
        if (zookeeperClient.checkExists().forPath(ZK_BASE_PATH) == null) {
            zookeeperClient.create().forPath(ZK_BASE_PATH);
        }
    }

    private void watchGlobalConfig() throws Exception {
        this.globalConfigWatcher = new NodeCache(getZookeeperClient(), ZK_CONF_PATH);
        this.globalConfigWatcher.getListenable().addListener(() -> {
            byte[] data = getZookeeperClient().getData().forPath(ZK_CONF_PATH);
            GlobalConfig globalConfig = (GlobalConfig) Codec.decode(data);
            if (globalConfig.getReplication() != this.globalConfig.getReplication()) {
                // TODO
                this.globalConfig = globalConfig;
            }
        });
        this.globalConfigWatcher.start();
    }

    private void writeGlobalConfigIfNotExist() throws Exception {
        CuratorFramework client = this.getZookeeperClient();
        GlobalConfig globalConfig = CeresFS.getContext().getBean(GlobalConfig.class);
        if (client.checkExists().forPath(ZK_CONF_PATH) == null) {
            this.globalConfig = globalConfig;
            checkAndWriteGlobalConfig(globalConfig);
        } else {
            byte[] data = getZookeeperClient().getData().forPath(ZK_CONF_PATH);
            this.globalConfig = (GlobalConfig) Codec.decode(data);
            LOG.warn("{} is discarded because zookeeper already has one", globalConfig);
        }
    }

    private void createDisksIfNotExist() throws IOException {
        for (Disk disk : localConfig.getDisks()) {
            String path = disk.getPath();
            File directory = new File(path);
            if (!directory.exists()) {
                directory.mkdirs();
            } else if (!directory.isDirectory()) {
                throw new IOException(path + " exists and is not a directory.");
            }
        }
    }

    private void checkAndWriteGlobalConfig(GlobalConfig globalConfig) throws Exception {
        checkGlobalConfig(globalConfig);
        getZookeeperClient().create().forPath(ZK_CONF_PATH, Codec.encode(globalConfig));
    }

    private void checkGlobalConfig(GlobalConfig globalConfig) {
        // TODO
    }

    private void checkLocalConfig(LocalConfig localConfig) {
        // TODO
    }

    @Override
    public void destroy() throws Exception {
        this.globalConfigWatcher.close();
        this.zookeeperClient.close();
    }


    @Bean
    protected GlobalConfig loadGlobalConfig() {
        return new GlobalConfig();
    }

    @Bean
    protected LocalConfig loadLocalConfig() {
        return new LocalConfig();
    }


    @ConfigurationProperties("ceresfs")
    protected static class GlobalConfig implements Serializable {

        private byte replication;
        private int vnodeFactor;

        public byte getReplication() {
            return replication;
        }

        public void setReplication(byte replication) {
            this.replication = replication;
        }

        public int getVnodeFactor() {
            return vnodeFactor;
        }

        public void setVnodeFactor(int vnodeFactor) {
            this.vnodeFactor = vnodeFactor;
        }

        @Override
        public String toString() {
            return "GlobalConfig{" +
                    "replication=" + replication +
                    ", vnodeFactor=" + vnodeFactor +
                    '}';
        }
    }

    @ConfigurationProperties("ceresfs")
    protected static class LocalConfig implements Serializable {
        private short id;
        private int port;
        private String zookeeperAddress;
        private double diskDefaultWeight;
        private long volumeMaxSize;
        private double volumeCompactThreshold;
        private String volumeCompactPeriodTimeunit;
        private long volumeCompactPeriod;
        private int imageMaxSize;
        private List<Disk> disks;

        public short getId() {
            return id;
        }

        public void setId(short id) {
            this.id = id;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getZookeeperAddress() {
            return zookeeperAddress;
        }

        public void setZookeeperAddress(String zookeeperAddress) {
            this.zookeeperAddress = zookeeperAddress;
        }

        public double getDiskDefaultWeight() {
            return diskDefaultWeight;
        }

        public void setDiskDefaultWeight(double diskDefaultWeight) {
            this.diskDefaultWeight = diskDefaultWeight;
        }

        public long getVolumeMaxSize() {
            return volumeMaxSize;
        }

        public void setVolumeMaxSize(long volumeMaxSize) {
            this.volumeMaxSize = volumeMaxSize;
        }

        public double getVolumeCompactThreshold() {
            return volumeCompactThreshold;
        }

        public void setVolumeCompactThreshold(double volumeCompactThreshold) {
            this.volumeCompactThreshold = volumeCompactThreshold;
        }

        public String getVolumeCompactPeriodTimeunit() {
            return volumeCompactPeriodTimeunit;
        }

        public void setVolumeCompactPeriodTimeunit(String volumeCompactPeriodTimeunit) {
            this.volumeCompactPeriodTimeunit = volumeCompactPeriodTimeunit;
        }

        public long getVolumeCompactPeriod() {
            return volumeCompactPeriod;
        }

        public void setVolumeCompactPeriod(long volumeCompactPeriod) {
            this.volumeCompactPeriod = volumeCompactPeriod;
        }

        public int getImageMaxSize() {
            return imageMaxSize;
        }

        public void setImageMaxSize(int imageMaxSize) {
            this.imageMaxSize = imageMaxSize;
        }

        public List<Disk> getDisks() {
            return disks;
        }

        public void setDisks(List<Disk> disks) {
            this.disks = disks;
        }

        @Override
        public String toString() {
            return "LocalConfig{" +
                    "id=" + id +
                    ", port=" + port +
                    ", zookeeperAddress='" + zookeeperAddress + '\'' +
                    ", diskDefaultWeight=" + diskDefaultWeight +
                    ", volumeMaxSize=" + volumeMaxSize +
                    ", imageMaxSize=" + imageMaxSize +
                    ", disks=" + disks +
                    '}';
        }
    }
}

