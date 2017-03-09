package io.github.huiyu.ceresfs.cache;

import io.github.huiyu.ceresfs.storage.Image;

public interface Cache {

    Image get(long id);

    void put(Image image);

    void evict(long id);
}
