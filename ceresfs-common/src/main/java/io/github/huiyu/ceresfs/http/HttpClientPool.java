package io.github.huiyu.ceresfs.http;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.github.huiyu.ceresfs.Const;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class HttpClientPool implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientPool.class);

    private int aggregatorBufferSize;

    private Cache<String, HttpClient> cache = CacheBuilder.newBuilder()
            .removalListener((RemovalListener<String, HttpClient>) notification -> {
                LOG.debug("HttpClient[{}] expired", notification.getKey());
                notification.getValue().shutdown();
            })
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .concurrencyLevel(4)
            .build();

    public HttpClientPool() {
        this.aggregatorBufferSize = Const.MAX_IMAGE_SIZE + 8192;
    }

    public HttpClient getOrCreate(String host, int port) {
        try {
            String key = host + ":" + port;
            return cache.get(key, () -> {
                LOG.debug("HttpClient[{}] created", key);
                return new HttpClient.Builder(host, port)
                        .aggregateBufferSize(aggregatorBufferSize)
                        .build();
            });
        } catch (ExecutionException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                IOException cause = (IOException) e.getCause();
                throw new UncheckedIOException(cause);
            }
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
    }
}

