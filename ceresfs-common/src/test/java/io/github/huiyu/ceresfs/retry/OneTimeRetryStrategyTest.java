package io.github.huiyu.ceresfs.retry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static org.junit.Assert.assertEquals;

public class OneTimeRetryStrategyTest {

    private Level level;

    @Before
    public void setUp() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        level = root.getLevel();
        root.setLevel(Level.OFF);
    }

    @After
    public void tearDown() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(level);
    }

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