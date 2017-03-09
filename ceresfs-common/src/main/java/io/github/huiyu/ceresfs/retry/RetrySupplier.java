package io.github.huiyu.ceresfs.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RetrySupplier<T> implements Supplier<T> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RetrySupplier.class);

    private final Supplier<T> task;
    private final RetryStrategy strategy;

    public RetrySupplier(Supplier<T> task, RetryStrategy strategy) {
        this.task = task;
        this.strategy = strategy;
    }

    @Override
    public T get() {
        try {
            return task.get();
        } catch (Exception e) {
            while (strategy.allowRetry()) try {
                return task.get();
            } catch (Exception ex) {
                LOG.error("Retry " + task + " error", e);
            }
            throw e;
        }
    }
}
