package com.supconit.ceresfs.topology;

import java.io.Serializable;
import java.util.List;

public class Node implements Serializable {

    private short id;
    private String hostName;
    private String hostAddress;
    private int port;
    private boolean balanced;

    private List<Disk> disks;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<Disk> getDisks() {
        return disks;
    }

    public void setDisks(List<Disk> disks) {
        this.disks = disks;
    }

    public void setId(short id) {
        this.id = id;
    }

    public short getId() {
        return id;
    }

    public boolean isBalanced() {
        return balanced;
    }

    public void setBalanced(boolean balanced) {
        this.balanced = balanced;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", hostName='" + hostName + '\'' +
                ", hostAddress='" + hostAddress + '\'' +
                ", port=" + port +
                '}';
    }
}
