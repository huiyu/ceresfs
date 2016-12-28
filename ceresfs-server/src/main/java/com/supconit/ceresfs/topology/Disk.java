package com.supconit.ceresfs.topology;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class Disk implements Serializable {

    private String path;
    private double weight;

    private Node node;

    private List<VNode> vnodes;

    public Disk() {
    }

    public Disk(String path, double weight) {
        this.path = path;
        this.weight = weight;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public List<VNode> getVnodes() {
        return vnodes;
    }

    public void setVnodes(List<VNode> vnodes) {
        this.vnodes = vnodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Disk disk = (Disk) o;
        return Objects.equals(path, disk.path) &&
                Objects.equals(node, disk.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, node);
    }
}
