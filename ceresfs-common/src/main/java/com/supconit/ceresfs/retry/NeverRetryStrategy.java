package com.supconit.ceresfs.retry;

public class NeverRetryStrategy implements RetryStrategy {
    
    @Override
    public boolean allowRetry() {
        return false;
    }
}
