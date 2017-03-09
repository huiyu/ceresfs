package io.github.huiyu.ceresfs.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.*;

public class NetUtilTest {

    private TestingServer server;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer(true);
    }

    @After
    public void tearDown() throws Exception {
        server.close();
    }

    @Test
    public void testGetLocalAddressFromZookeeper() throws Exception {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), retryPolicy);
        client.start();
        InetAddress address = NetUtil.getLocalAddressFromZookeeper(client);
        assertNotNull(address);
        assertEquals("127.0.0.1", address.getHostAddress());
        client.close();
    }
}