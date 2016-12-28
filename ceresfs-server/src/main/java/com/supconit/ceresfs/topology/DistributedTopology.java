package com.supconit.ceresfs.topology;

import com.google.common.primitives.Longs;

import com.supconit.ceresfs.CeresFSConfiguration;
import com.supconit.ceresfs.util.HashUtil;
import com.supconit.ceresfs.util.NetUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@Component
@ConditionalOnProperty(prefix = "ceresfs", name = "mode", havingValue = "distributed", matchIfMissing = true)
public class DistributedTopology implements Topology, InitializingBean, DisposableBean {

    private static final String ZK_BASE_PATH = "/ceresfs";
    private static final int ZNODE_SIZE_LIMIT = 1024 * 1024 * 1024;

    private final TreeMap<Long, Disk> hashCircle = new TreeMap<>();

    private Node localNode;
    private List<Node> allNodes;
    private CuratorFramework zookeeperClient;
    private PersistentNode persistentNode;
    private PathChildrenCache pathChildrenCache;

    private NodeCodec nodeCodec;
    private CeresFSConfiguration configuration;

    @Autowired
    public DistributedTopology(CeresFSConfiguration configuration, NodeCodec nodeCodec) {
        this.configuration = configuration;
        this.nodeCodec = nodeCodec;
    }

    @Override
    public Mode mode() {
        return Mode.DISTRIBUTED;
    }

    @Override
    public Node localNode() {
        return localNode;
    }

    @Override
    public List<Node> allNodes() {
        return allNodes;
    }

    @Override
    public Disk route(byte[] id) {
        SortedMap<Long, Disk> tail = hashCircle.tailMap(HashUtil.murmur(id));
        if (tail.size() == 0) {
            return hashCircle.get(hashCircle.firstKey());
        } else {
            return tail.get(tail.firstKey());
        }
    }

    @Override
    public Disk route(long id) {
        return route(Longs.toByteArray(id));
    }

    @Override
    public void destroy() throws Exception {
        pathChildrenCache.close();
        persistentNode.close();
        zookeeperClient.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        createZKClient();
        initLocalNode();
        registerNode();
        watchNodes();
    }

    private void createZKClient() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        zookeeperClient = CuratorFrameworkFactory
                .newClient(configuration.getZookeeperQuorum(), retryPolicy);
        zookeeperClient.start();
    }

    private void initLocalNode() throws IOException {
        Node node = new Node();
        node.setPort(configuration.getPort());
        InetAddress localHost = getLocalHostFromZK(configuration.getZookeeperQuorum());
        node.setHostAddress(localHost.getHostAddress());
        node.setHostName(localHost.getHostName());
        List<Disk> disks = getLocalDisks(configuration.getDisks());

        node.setDisks(disks);

        for (Disk disk : disks) {
            disk.setNode(node);

            File f = new File(disk.getPath());
            if (!f.exists()) {
                f.mkdirs();
            } else if (!f.isDirectory()) {
                throw new IOException("Path " + disk.getPath() + " exists and is not a directory.");
            }
            int numVNodes = (int) (disk.getWeight() * configuration.getVnodeFactor());

            List<VNode> vnodes = new ArrayList<>(numVNodes);
            for (int i = 0; i < numVNodes; i++) {
                String key = node.getHostAddress() + ":" + node.getPort() + ":" + disk.getPath() + ":" + i;
                // FIXME: need to check duplicated id
                long id = HashUtil.murmur(key.getBytes());
                VNode vnode = new VNode(id);
                vnodes.add(vnode);
            }
            disk.setVnodes(vnodes);
        }
        this.localNode = node;
    }

    private void registerNode() throws IOException {
        Node node = this.localNode;
        String id = node.getHostAddress() + ":" + node.getPort();
        String path = ZKPaths.makePath(ZK_BASE_PATH, "nodes", id);
        byte[] data = nodeCodec.encode(node);
        if (data.length >= ZNODE_SIZE_LIMIT) {
            throw new IOException("Node size is " + data.length +
                    " which is over ZNode size limit(1MB), please try to decrease vnode number");
        }
        this.persistentNode = new PersistentNode(
                zookeeperClient, CreateMode.EPHEMERAL, false, path, data);
        this.persistentNode.start();
    }

    private void watchNodes() throws Exception {
        String zkNodesPath = ZKPaths.makePath(ZK_BASE_PATH, "nodes");
        pathChildrenCache = new PathChildrenCache(zookeeperClient, zkNodesPath, false);
        pathChildrenCache.getListenable().addListener((client, event) -> {
            List<String> children = zookeeperClient.getChildren().forPath(zkNodesPath);
            allNodes = new ArrayList<>(children.size());
            for (String child : children) {
                byte[] data = client.getData().forPath(ZKPaths.makePath(zkNodesPath, child));
                Node node = nodeCodec.decode(data);
                allNodes.add(node);
                for (Disk disk : node.getDisks()) {
                    for (VNode vNode : disk.getVnodes()) {
                        hashCircle.put(vNode.getId(), vNode.getDisk());
                    }
                }
            }
        });
        pathChildrenCache.start();
    }

    private List<Disk> getLocalDisks(String diskProperty) {
        Map<String, String> weightByPath = parseMapProperties(diskProperty);
        List<Disk> disks = new ArrayList<>(weightByPath.size());
        for (Map.Entry<String, String> entry : weightByPath.entrySet()) {
            String path = entry.getKey();
            double weight = entry.getValue() == null ?
                    configuration.getDiskDefaultWeight() :
                    Double.parseDouble(entry.getValue());
            Disk disk = new Disk();
            disk.setPath(path);
            disk.setWeight(weight);
            disks.add(disk);
        }
        return disks;
    }


    private InetAddress getLocalHostFromZK(String zkQuorum) throws IOException {
        Map<String, String> portByHost = parseMapProperties(zkQuorum, "2181");
        for (Map.Entry<String, String> entry : portByHost.entrySet()) {
            String host = entry.getKey();
            int port = Integer.parseInt(entry.getValue());
            InetAddress address = getLocalHostFromZK(host, port);
            if (address != null) {
                return address;
            }
        }
        throw new IOException("Can't connect to zookeeper " + zkQuorum);
    }

    private InetAddress getLocalHostFromZK(String zkHost, int zkPort) {
        try {
            return NetUtil.getLocalHostFromRemoteServer(zkHost, zkPort);
        } catch (IOException e) {
            return null;
        }
    }

    private Map<String, String> parseMapProperties(String properties) {
        return parseMapProperties(properties, null);
    }

    private Map<String, String> parseMapProperties(String properties, String defaultValue) {
        String[] props = properties.split(";");
        Map<String, String> result = new HashMap<>(props.length);
        for (String prop : props) {
            String[] pair = prop.split(":");
            Assert.isTrue(pair.length <= 2, "Invalid property format: " + defaultValue);
            String key = pair[0];
            String val = pair.length > 1 ? pair[1] : defaultValue;
            result.put(key, val);
        }
        return result;
    }
}
