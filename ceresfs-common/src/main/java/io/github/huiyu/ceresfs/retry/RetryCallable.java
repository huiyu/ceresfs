package io.github.huiyu.ceresfs.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class RetryCallable<T> implements Callable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RetryCallable.class);

    private final Callable<T> task;
    private final RetryStrategy strategy;

    public RetryCallable(Callable<T> task, RetryStrategy strategy) {
        this.task = task;
        this.strategy = strategy;
    }

    @Override
    public T call() throws Exception {
        try {
            return task.call();
        } catch (Exception e) {
            while (strategy.allowRetry()) try {
                return task.call();
            } catch (Exception ex) {
                LOG.error("Retry " + task + " error", e);
            }
            throw e;
        }
    }
}
