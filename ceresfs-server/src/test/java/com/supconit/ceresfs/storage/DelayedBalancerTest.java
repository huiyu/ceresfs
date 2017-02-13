package com.supconit.ceresfs.storage;

import com.google.common.util.concurrent.UncheckedExecutionException;

import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.topology.Topology;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DelayedBalancerTest {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedBalancerTest.class);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        tempFolder.create();
    }

    @After
    public void tearDown() throws Exception {
        tempFolder.delete();
    }

    @Test
    public void testCancel() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        Balancer balancer = new DelayedBalancer(null, null, null, null) {

            @Override
            protected void run() {
                count.set(0);
                while (isRunning()) {
                    await(1000L);
                    int x = count.incrementAndGet();
                    LOG.info("Count: {}", x);
                }
                throw new UncheckedExecutionException(new InterruptedException());
            }

            private synchronized void await(long timeMillis) {
                try {
                    this.wait(timeMillis);
                } catch (InterruptedException e) {
                }
            }
        };

        // cancel when waiting
        balancer.start(2000, TimeUnit.MILLISECONDS);
        balancer.cancel();
        Thread.sleep(2000L);
        assertEquals(0, count.get());

        // cancel when executing
        balancer.start(0, TimeUnit.MILLISECONDS);
        Thread.sleep(2000L);
        balancer.cancel();
        assertFalse(balancer.isRunning());
        assertTrue(count.get() > 0);
    }
}