package com.supconit.ceresfs;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ImageType {

    JPG("image/jpeg", (byte) 1),
    PNG("image/png", (byte) 2),
    GIF("image/gif", (byte) 3),
    BMP("image/bmp", (byte) 4);

    private String mimeType;
    private byte code;

    ImageType(String mimeType, byte code) {
        this.mimeType = mimeType;
        this.code = code;
    }

    public static ImageType fromFileName(String fileName) {
        checkNotNull(fileName);
        String[] tokens = fileName.split("\\.");
        String suffix = tokens[tokens.length - 1].toUpperCase();
        switch (suffix) {
            case "JPEG":
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

    public static ImageType fromCode(byte code) {
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

    public static ImageType fromMimeType(String mimeType) {
        for (ImageType type : ImageType.values()) {
            if (type.getMimeType().equals(mimeType))
                return type;
        }
        throw new IllegalArgumentException("Unknown mime type: " + mimeType);
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
