package com.supconit.ceresfs.client;

import com.supconit.ceresfs.ImageType;

public class Image {

    private long id;
    private ImageType type;
    private long expireTime;
    private byte[] data;

    public Image() {
    }

    public Image(long id, ImageType type, long expireTime, byte[] data) {
        this.id = id;
        this.type = type;
        this.expireTime = expireTime;
        this.data = data;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ImageType getType() {
        return type;
    }

    public void setType(ImageType type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }
}

