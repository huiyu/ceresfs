package io.github.huiyu.ceresfs.retry;

public class NeverRetryStrategy implements RetryStrategy {
    
    @Override
    public boolean allowRetry() {
        return false;
    }
}
