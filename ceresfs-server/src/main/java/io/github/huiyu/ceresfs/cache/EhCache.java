package io.github.huiyu.ceresfs.cache;

import io.github.huiyu.ceresfs.storage.Image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "ceresfs.cache", name = "mode", havingValue = "ehcache")
public class EhCache implements Cache {

    private static final Logger LOG = LoggerFactory.getLogger(EhCache.class);

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
