package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.storage.Image;

public class ImageSaveError {

    private Image image;
    private Throwable error;

    public ImageSaveError(Image image, Throwable error) {
        this.image = image;
        this.error = error;
    }

    public Image getImage() {
        return image;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }
}
