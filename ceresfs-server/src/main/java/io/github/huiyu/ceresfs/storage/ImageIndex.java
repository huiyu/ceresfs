package io.github.huiyu.ceresfs.storage;

import io.github.huiyu.ceresfs.ImageType;

import java.io.Serializable;

public class ImageIndex implements Serializable { // total 47 byte
   
    public static final byte FLAG_NORMAL = 0;
    public static final byte FLAG_DELETED = 1;

    // should never change this value.
    public static final int FIXED_LENGTH = 64;

    private long id; // 8 byte
    private long volume; // 8 byte
    private byte flag; // 1 byte
    private long offset; // 8 byte
    private int size; // 4 byte
    private ImageType type; // 1byte

    private long time; // 8 byte
    private long expireTime; // 8 byte

    private byte replication; // 1 byte

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ImageType getType() {
        return type;
    }

    public void setType(ImageType type) {
        this.type = type;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public byte getReplication() {
        return replication;
    }

    public void setReplication(byte replication) {
        this.replication = replication;
    }

    @Override
    public String toString() {
        return "Index{" +
                "id=" + id +
                ", volume=" + volume +
                ", flag=" + flag +
                ", offset=" + offset +
                ", size=" + size +
                ", type=" + type +
                ", time=" + time +
                ", expireTime=" + expireTime +
                ", replication=" + replication +
                '}';
    }
}
