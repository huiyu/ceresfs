package com.supconit.ceresfs.http;

import com.supconit.ceresfs.topology.Node;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class HttpClientPool extends GenericKeyedObjectPool<Node, HttpClient>
        implements InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientPool.class);

    public HttpClientPool() {
        super(new HttpClientPoolFactory(), createPoolConfig());
    }

    private static GenericKeyedObjectPoolConfig createPoolConfig() {
        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setMaxIdlePerKey(100);
        config.setMinIdlePerKey(0);
        return config;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        HttpClientPoolFactory factory = (HttpClientPoolFactory) this.getFactory();
        factory.setPool(this);
    }

    @Override
    public void destroy() throws Exception {
        this.close();
    }

    private static class HttpClientPoolFactory extends BaseKeyedPooledObjectFactory<Node, HttpClient> {

        private final Map<Node, HttpClient> clients = new ConcurrentHashMap<>();
        private HttpClientPool pool;

        @Override
        public synchronized HttpClient create(Node key) throws Exception {
            return clients.computeIfAbsent(key,
                    k -> {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("HttpClient of {} is created", k.toString());
                        }
                        return new HttpClient(key.getHostAddress(), key.getPort());
                    });
        }

        @Override
        public PooledObject<HttpClient> wrap(HttpClient value) {
            return new DefaultPooledObject<>(value);
        }

        @Override
        public synchronized void destroyObject(Node key, PooledObject<HttpClient> p)
                throws Exception {
            final HttpClientPool pool = getPool();
            long borrowedCount = pool.getBorrowedCount();
            if (borrowedCount < 1) {
                clients.remove(key);
                p.getObject().shutdown();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("HttpClient of {} is destroyed", key.toString());
                }
            }
        }

        public HttpClientPool getPool() {
            return pool;
        }

        public void setPool(HttpClientPool pool) {
            this.pool = pool;
        }
    }
}
