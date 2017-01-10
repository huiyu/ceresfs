package com.supconit.ceresfs.retry;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class NTimesRetryStrategyTest {

    @Test
    public void test() throws Exception {
        int times = 2;
        AtomicInteger count = new AtomicInteger(0);
        RetrySupplier<String> supplier = new RetrySupplier<>(() -> {
            count.incrementAndGet();
            throw new RuntimeException();
        }, new NTimesRetryStrategy(times, 10L));
        try {
            supplier.get();
        } catch (Exception e) {
        } finally {
            assertEquals(times + 1, count.get());
        }
    }
}