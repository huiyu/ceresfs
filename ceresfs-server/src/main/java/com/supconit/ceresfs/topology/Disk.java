package com.supconit.ceresfs.topology;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

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
}
