package com.supconit.ceresfs.client;

public class Image {

    private final long id;
    private final String mimeType;
    private final byte[] data;

    public Image(long id, String mimeType, byte[] data) {
        this.id = id;
        this.mimeType = mimeType;
        this.data = data;
    }

    public long getId() {
        return id;
    }

    public String getMimeType() {
        return mimeType;
    }

    public byte[] getData() {
        return data;
    }
}
