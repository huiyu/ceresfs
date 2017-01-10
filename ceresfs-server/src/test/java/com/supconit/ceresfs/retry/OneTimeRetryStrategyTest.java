package com.supconit.ceresfs.retry;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class OneTimeRetryStrategyTest {

    @Test
    public void test() throws Exception {
        AtomicInteger count = new AtomicInteger(0);
        RetrySupplier<String> supplier = new RetrySupplier<>(() -> {
            count.getAndIncrement();
            throw new RuntimeException();
        }, new OneTimeRetryStrategy(100L));
        try {
            supplier.get();
        } catch (Exception e) {
        } finally {
            assertEquals(2, count.get());
        }
    }
}