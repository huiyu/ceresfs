package io.github.huiyu.ceresfs.retry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static org.junit.Assert.*;

public class NTimesRetryStrategyTest {

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