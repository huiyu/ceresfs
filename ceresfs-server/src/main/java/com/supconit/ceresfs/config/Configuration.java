package com.supconit.ceresfs.config;

import com.supconit.ceresfs.topology.Disk;

import org.apache.curator.framework.CuratorFramework;

import java.util.List;

public interface Configuration {

    short getId();

    int getPort();

    double getDiskDefaultWeight();

    long getVolumeMaxSize();

    int getImageMaxSize();

    int getVnodeFactor();

    List<Disk> getDisks();
    
    String getZookeeperAddress();

    CuratorFramework getZookeeperClient();

    byte getReplication();

    void setReplication(byte replication);
}
