package com.supconit.ceresfs.cache;

import com.supconit.ceresfs.storage.Image;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "ceresfs.cache", name = "mode", havingValue = "none", matchIfMissing = true)
public class NoCache implements Cache {

    @Override
    public Image get(long id) {
        return null;
    }

    @Override
    public void put(Image image) {

    }

    @Override
    public void evict(long id) {

    }
}
