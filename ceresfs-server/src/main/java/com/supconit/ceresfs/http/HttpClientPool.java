package com.supconit.ceresfs.http;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
public class HttpClientPool implements DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientPool.class);

    private static Cache<String, HttpClient> CACHE = CacheBuilder.newBuilder()
            .removalListener((RemovalListener<String, HttpClient>) notification -> {
                LOG.debug("HttpClient[{}] expired", notification.getKey());
                notification.getValue().shutdown();
            })
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .concurrencyLevel(4)
            .build();

    public static HttpClient getOrCreate(String host, int port) {
        try {
            String key = host + ":" + port;
            return CACHE.get(key, () -> {
                LOG.debug("HttpClient[{}] created", key);
                return new HttpClient(host, port);
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
    public void destroy() throws Exception {
        CACHE.cleanUp();
    }
}

