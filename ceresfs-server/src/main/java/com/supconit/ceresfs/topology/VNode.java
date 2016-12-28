package com.supconit.ceresfs.topology;

import java.io.Serializable;

/**
 * Virtual Node
 */
public class VNode implements Serializable {

    private long id;
    private Disk disk;

    public VNode() {
    }

    public VNode(long id) {
        this.id = id;
    }

    public VNode(long id, Disk disk) {
        this.id = id;
        this.disk = disk;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Disk getDisk() {
        return disk;
    }

    public void setDisk(Disk disk) {
        this.disk = disk;
    }
}
