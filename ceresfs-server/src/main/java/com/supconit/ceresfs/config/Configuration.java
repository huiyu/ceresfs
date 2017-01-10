package com.supconit.ceresfs.config;

import com.supconit.ceresfs.topology.Disk;

import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface Configuration {

    short getId();

    int getPort();

    double getDiskDefaultWeight();

    long getVolumeMaxSize();
    
    double getVolumeCompactThreshold();
    
    long getVolumeCompactPeriod();
    
    TimeUnit getVolumeCompactPeriodTimeUnit();
    
    int getImageMaxSize();

    int getVnodeFactor();

    List<Disk> getDisks();
    
    String getZookeeperAddress();

    CuratorFramework getZookeeperClient();

    byte getReplication();

    void setReplication(byte replication) throws Exception;
}
