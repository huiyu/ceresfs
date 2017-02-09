package com.supconit.ceresfs.topology;

import com.supconit.ceresfs.Const;
import com.supconit.ceresfs.config.Configuration;
import com.supconit.ceresfs.storage.Balancer;
import com.supconit.ceresfs.util.Codec;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class DistributedTopologyTest {

    private static TemporaryFolder FOLDER = new TemporaryFolder();
    private static TestingServer SERVER;
    private static Configuration CONFIG;
    private static Balancer BALANCER;
    private static CuratorFramework CLIENT;
    private static DistributedTopology TOPOLOGY;

    @BeforeClass
    public static void setUp() throws Exception {
        FOLDER.create();
        SERVER = new TestingServer(true);
        String zookeeperQuorum = SERVER.getConnectString();
        RetryPolicy neverRetry = (retryCount, elapsedTimeMs, sleeper) -> false;
        CLIENT = CuratorFrameworkFactory.newClient(zookeeperQuorum, neverRetry);
        CLIENT.start();
        // mock configuration
        CONFIG = mockConfig(zookeeperQuorum);
        // mock balancer
        BALANCER = new AlwaysSuccessBalancer();
        // create topology
        TOPOLOGY = new DistributedTopology(CONFIG, BALANCER);
        TOPOLOGY.afterPropertiesSet();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        TOPOLOGY.destroy();
        SERVER.close();
    }

    private static Configuration mockConfig(String zookeeperQuorum) {
        Configuration config = mock(Configuration.class);
        when(config.getId()).thenReturn((short) 1);
        when(config.getDiskDefaultWeight()).thenReturn(1.0);
        when(config.getZookeeperAddress()).thenReturn(zookeeperQuorum);
        when(config.getZookeeperClient()).thenReturn(CLIENT);
        Disk disk = new Disk(((short) 0), FOLDER.getRoot().getAbsolutePath(), 1.0);
        List<Disk> disks = Collections.singletonList(disk);
        when(config.getDisks()).thenReturn(disks);
        when(config.getPort()).thenReturn(9900);
        return config;
    }

    @Test
    public void testLocalHost() throws Exception {
        Node node = TOPOLOGY.getLocalNode();
        assertEquals(((short) 1), node.getId());
        assertEquals(9900, node.getPort());
        assertEquals(1, node.getDisks().size());

        Disk disk = node.getDisks().get(0);
        assertEquals((short) 0, disk.getId());
        assertEquals(FOLDER.getRoot().getAbsolutePath(), disk.getPath());
        assertEquals(1.0, disk.getWeight(), 0.0);

        List<Node> nodes = TOPOLOGY.getAllNodes();
        assertEquals(1, nodes.size());
        byte[] bytes = CLIENT.getData().forPath(
                ZKPaths.makePath(Const.ZK_NODES_PATH, String.valueOf(node.getId())));
        assertArrayEquals(Codec.encode(node), bytes);
    }

    @Test
    public void testTopologyChangeListener() {
        // TODO
    }

    private static class AlwaysSuccessBalancer implements Balancer {

        private volatile boolean running;

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public CompletableFuture<Void> start(long delay, TimeUnit delayTimeUnit) {
            if (isRunning()) {
                throw new IllegalStateException("Balancer is running");
            }

            running = true;
            return CompletableFuture.runAsync(() -> {
                return;
            });
        }

        @Override
        public void cancel() {
        }
    }
}