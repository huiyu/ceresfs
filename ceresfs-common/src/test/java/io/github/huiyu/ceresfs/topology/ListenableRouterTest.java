package io.github.huiyu.ceresfs.topology;

import io.github.huiyu.ceresfs.Const;
import io.github.huiyu.ceresfs.util.Codec;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ListenableRouterTest {

    private TestingServer testingServer;
    private CuratorFramework zookeeperClient;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        tempFolder.create();

        testingServer = new TestingServer(true);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        zookeeperClient = CuratorFrameworkFactory.newClient(
                testingServer.getConnectString(), retryPolicy);
        zookeeperClient.start();
        zookeeperClient.create().forPath(Const.ZK_BASE_PATH);
        zookeeperClient.create().forPath(Const.ZK_NODES_PATH);
        Node node = new Node();
        node.setId((short) 1);
        node.setBalanced(true);
        node.setHostAddress("10.10.100.2");
        node.setHostName("host1");

        File diskFolder = tempFolder.newFolder();
        Disk disk = new Disk((short) 1, diskFolder.getAbsolutePath(), 1.0);
        disk.setNode(node);
        List<Disk> diskList = new ArrayList<>();
        diskList.add(disk);
        node.setDisks(diskList);
        zookeeperClient.create().forPath(Const.makeZKNodePath(node.getId()), Codec.encode(node));
    }

    @After
    public void tearDown() throws Exception {
        zookeeperClient.close();
        testingServer.close();
        tempFolder.delete();
    }

    @Test
    public void testTopologyChangeListener() throws Exception {
        ListenableRouter router = new ListenableRouter(zookeeperClient, 1000);
        List<Node> nodes = router.getNodes();
        assertEquals(1, nodes.size());
        assertEquals((short) 1, nodes.get(0).getId());

        // add listener
        TestTopologyChangedListener listener = new TestTopologyChangedListener();
        router.addTopologyChangeListener(listener);

        // test add node
        Node node = new Node();
        node.setId((short) 2);
        node.setHostAddress("10.10.100.3");
        node.setHostName("host2");
        node.setBalanced(false);

        List<Disk> disks = new ArrayList<>();
        node.setDisks(disks);

        Disk disk = new Disk((short) 1, tempFolder.newFolder().getAbsolutePath(), 2.0);
        disk.setNode(node);
        disks.add(disk);
        zookeeperClient.create().forPath(Const.makeZKNodePath(node.getId()), Codec.encode(node));

        Thread.sleep(200L);
        assertEquals(2, router.getNodes().size());
        assertEquals(1, listener.nodesAdded.size());
        assertEquals((short) 2, listener.nodesAdded.get(0).getId());
        assertEquals("10.10.100.3", listener.nodesAdded.get(0).getHostAddress());

        // test add disk
        Disk diskAdd = new Disk((short) 2, tempFolder.newFolder().getAbsolutePath(), 1.0);
        diskAdd.setNode(node);
        node.getDisks().add(diskAdd);
        zookeeperClient.setData().forPath(Const.makeZKNodePath(node.getId()), Codec.encode(node));

        Thread.sleep(200L);
        assertEquals(2, router.getNodes().size());
        assertEquals(1, listener.disksAdded.size());

        // test remove disk
        // remove last
        node.getDisks().remove(node.getDisks().size() - 1);
        zookeeperClient.setData().forPath(Const.makeZKNodePath(node.getId()), Codec.encode(node));
        Thread.sleep(200L);
        assertEquals(2, router.getNodes().size());
        assertEquals(1, listener.disksRemoved.size());

        // test on node balanced
        node.setBalanced(true);
        zookeeperClient.setData().forPath(Const.makeZKNodePath((short) 2), Codec.encode(node));
        Thread.sleep(200L);
        assertTrue(listener.onNodeBalanceInvoked);

        // test remove node
        zookeeperClient.delete().forPath(Const.makeZKNodePath(node.getId()));
        Thread.sleep(200L);
        assertEquals(1, router.getNodes().size());
        assertEquals(1, listener.nodesRemoved.size());
        assertEquals((short) 2, listener.nodesRemoved.get(0).getId());


        router.close();
    }

    static class TestTopologyChangedListener implements TopologyChangeListener {

        List<Node> nodesAdded = new ArrayList<>();
        List<Node> nodesRemoved = new ArrayList<>();
        List<Disk> disksAdded = new ArrayList<>();
        List<Disk> disksRemoved = new ArrayList<>();
        Map<Disk, Double> diskWeightChanged = new HashMap<>();
        boolean onNodeBalanceInvoked = false;

        @Override
        public void onNodeAdded(Node node) {
            nodesAdded.add(node);
        }

        @Override
        public void onNodeRemoved(Node node) {
            nodesRemoved.add(node);
        }

        @Override
        public void onDiskAdded(Disk disk) {
            disksAdded.add(disk);
        }

        @Override
        public void onDiskRemoved(Disk disk) {
            disksRemoved.add(disk);
        }

        @Override
        public void onDiskWeightChanged(Disk disk, double newWeight) {
            diskWeightChanged.put(disk, newWeight);
        }

        @Override
        public void onNodeBalanceChanged(Node node) {
            this.onNodeBalanceInvoked = true;
        }
    }
}