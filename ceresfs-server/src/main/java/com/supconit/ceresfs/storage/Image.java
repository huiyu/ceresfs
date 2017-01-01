package com.supconit.ceresfs.storage;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class Image implements Serializable {

    public static final byte FLAG_NORMAL = 0;
    public static final byte FLAG_DELETED = 1;

    private Index index;
    private byte[] data;

    public Image() {
    }

    public Image(Index index, byte[] data) {
        this.index = index;
        this.data = data;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public enum Type {
        JPG("image/jpeg", (byte) 1),
        PNG("image/png", (byte) 2),
        GIF("image/gif", (byte) 3),
        BMP("image/bmp", (byte) 4);

        private String mimeType;
        private byte code;

        Type(String mimeType, byte code) {
            this.mimeType = mimeType;
            this.code = code;
        }

        public static Type parse(String fileName) {
            checkNotNull(fileName);
            String[] tokens = fileName.split("\\.");
            String suffix = tokens[tokens.length - 1].toUpperCase();
            switch (suffix) {
                case "JEPG":
                    return JPG;
                case "JPG":
                    return JPG;
                case "PNG":
                    return PNG;
                case "GIF":
                    return GIF;
                case "BMP":
                    return BMP;
                default:
                    throw new IllegalArgumentException("Unknown image type " + fileName);
            }
        }

        public static Type parse(byte code) {
            switch (code) {
                case 1:
                    return JPG;
                case 2:
                    return PNG;
                case 3:
                    return GIF;
                case 4:
                    return BMP;
                default:
                    throw new IllegalArgumentException("Unknown image code " + code);
            }
        }

        public String getMimeType() {
            return mimeType;
        }

        public byte getCode() {
            return code;
        }

        public String getFileSuffix() {
            return toString().toLowerCase();
        }
    }

    public static class Index implements Serializable { // total 47 byte

        // should never change this value.
        public static final int FIXED_LENGTH = 64;

        private long id; // 8 byte
        private long volume; // 8 byte
        private byte flag; // 1 byte
        private long offset; // 8 byte
        private int size; // 4 byte
        private Type type; // 1byte

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

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
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
}
