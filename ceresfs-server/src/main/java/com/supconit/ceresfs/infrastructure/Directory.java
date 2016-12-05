package com.supconit.ceresfs.infrastructure;

public class Directory {

    private String path;
    private double weight;

    public Directory() {
    }

    public Directory(String path, double weight) {
        this.path = path;
        this.weight = weight;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getPath() {
        return path;
    }

    public double getWeight() {
        return weight;
    }
}
