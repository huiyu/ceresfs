package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.EventHandler;

public interface ImageSaveTask {

    ImageSaveTask setExpireTime(long expireTime);

    ImageSaveTask onSuccess(EventHandler<Image> handler);

    ImageSaveTask onError(EventHandler<ImageSaveError> handler);

    void save(boolean sync);
}
