package com.supconit.ceresfs.retry;

import java.util.function.Supplier;

public class RetrySupplier<T> implements Supplier<T> {

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
                ex.printStackTrace();
            }
            throw e;
        }
    }
}
