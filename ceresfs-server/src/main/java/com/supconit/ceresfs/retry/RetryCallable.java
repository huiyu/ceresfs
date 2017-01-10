package com.supconit.ceresfs.retry;

import java.util.concurrent.Callable;

public class RetryCallable<T> implements Callable<T> {

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
                ex.printStackTrace();
            }
            throw e;
        }
    }
}
