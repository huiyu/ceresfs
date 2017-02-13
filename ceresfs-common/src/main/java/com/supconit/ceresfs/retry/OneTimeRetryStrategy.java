package com.supconit.ceresfs.retry;

public class OneTimeRetryStrategy implements RetryStrategy {

    private volatile boolean retried = false;

    private final long waitMillisecond;

    public OneTimeRetryStrategy(long waitMillisecond) {
        this.waitMillisecond = waitMillisecond;
    }

    @Override
    public boolean allowRetry() {
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }
        await();
        boolean retried = this.retried;
        this.retried = true;
        return !retried;
    }

    private synchronized void await() {
        try {
            this.wait(waitMillisecond);
        } catch (InterruptedException e) {
        }
    }
}
