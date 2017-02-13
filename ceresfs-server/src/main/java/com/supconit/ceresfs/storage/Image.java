package com.supconit.ceresfs.storage;

import java.io.Serializable;

public class Image implements Serializable {


    private ImageIndex index;
    private byte[] data;

    public Image() {
    }

    public Image(ImageIndex index, byte[] data) {
        this.index = index;
        this.data = data;
    }

    public ImageIndex getIndex() {
        return index;
    }

    public void setIndex(ImageIndex index) {
        this.index = index;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }


}
