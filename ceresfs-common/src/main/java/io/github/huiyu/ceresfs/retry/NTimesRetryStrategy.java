package io.github.huiyu.ceresfs.retry;

public class NTimesRetryStrategy implements RetryStrategy {

    private volatile int times;
    private final long millisecondBetweenSleeps;

    public NTimesRetryStrategy(int times, long millisecondBetweenSleeps) {
        this.times = times;
        this.millisecondBetweenSleeps = millisecondBetweenSleeps;
    }

    @Override
    public boolean allowRetry() {
        while (!Thread.currentThread().isInterrupted() && times > 0) {
            await();
            times--;
            return true;
        }
        return false;
    }

    private synchronized void await() {
        try {
            wait(millisecondBetweenSleeps);
        } catch (InterruptedException e) {
        }
    }
}
