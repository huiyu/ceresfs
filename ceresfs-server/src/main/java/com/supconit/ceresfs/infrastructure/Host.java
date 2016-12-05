package com.supconit.ceresfs.infrastructure;

import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.List;

public class Host implements Serializable {

    private String name;
    private String address;
    private int port;
    private List<Directory> directories;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<Directory> getDirectories() {
        return directories;
    }

    public void setDirectories(List<Directory> directories) {
        this.directories = directories;
    }

    public double getWeight() {
        return CollectionUtils.isEmpty(directories) ?
                0.0 :
                directories.stream().mapToDouble(Directory::getWeight).sum();
    }
}
