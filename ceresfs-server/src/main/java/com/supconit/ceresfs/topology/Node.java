package com.supconit.ceresfs.topology;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class Node implements Serializable {

    private short id;
    private String hostName;
    private String hostAddress;
    private int port;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return id == node.id &&
                port == node.port &&
                Objects.equals(hostName, node.hostName) &&
                Objects.equals(hostAddress, node.hostAddress) &&
                Objects.equals(disks, node.disks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, hostName, hostAddress, port, disks);
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
