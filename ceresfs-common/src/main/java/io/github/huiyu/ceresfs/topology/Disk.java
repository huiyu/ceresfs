package io.github.huiyu.ceresfs.topology;

import java.io.Serializable;

public class Disk implements Serializable {

    private short id;
    private String path;
    private double weight;

    private Node node;

    public Disk() {
    }

    public Disk(short id, String path, double weight) {
        this.id = id;
        this.path = path;
        this.weight = weight;
    }

    public short getId() {
        return id;
    }

    public void setId(short id) {
        this.id = id;
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

    @Override
    public String toString() {
        return "Disk{" +
                "id=" + id +
                ", path='" + path + '\'' +
                ", weight=" + weight +
                '}';
    }
}
