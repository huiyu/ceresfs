package com.supconit.ceresfs.retry;

public class ForeverRetryStrategy implements RetryStrategy {

    private final long millisecondBetweenSleeps;

    public ForeverRetryStrategy(long millisecondBetweenSleeps) {
        this.millisecondBetweenSleeps = millisecondBetweenSleeps;
    }

    @Override
    public boolean allowRetry() {
        await();
        return !Thread.currentThread().isInterrupted();
    }

    private synchronized void await() {
        try {
            wait(millisecondBetweenSleeps);
        } catch (InterruptedException e) {
        }
    }
}
