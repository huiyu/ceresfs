package com.supconit.ceresfs;

import com.supconit.ceresfs.topology.Disk;

import org.apache.curator.framework.CuratorFramework;

import java.util.List;

public interface Configuration {

    CuratorFramework getZookeeperClient();

    List<Disk> getDisks();

    short getId();

    int getPort();

    String getMode();

    String getZookeeperQuorum();

    double getDiskDefaultWeight();

    int getVnodeFactor();

    long getVolumeLimit();

    int getImageMaxSize();
}
