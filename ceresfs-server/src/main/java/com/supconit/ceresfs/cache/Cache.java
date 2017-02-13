package com.supconit.ceresfs.cache;

import com.supconit.ceresfs.storage.Image;

public interface Cache {

    Image get(long id);

    void put(Image image);

    void evict(long id);
}
