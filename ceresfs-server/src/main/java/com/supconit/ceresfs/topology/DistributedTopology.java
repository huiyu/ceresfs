package com.supconit.ceresfs.topology;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import com.supconit.ceresfs.Configuration;
import com.supconit.ceresfs.util.HashUtil;
import com.supconit.ceresfs.util.NetUtil;
import com.supconit.ceresfs.util.NumericUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

@Component
@ConditionalOnProperty(prefix = "ceresfs", name = "mode", havingValue = "distributed", matchIfMissing = true)
public class DistributedTopology implements Topology, InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedTopology.class);

    private static final String ZK_BASE_PATH = "/ceresfs";

    private FSTConfiguration fst = FSTConfiguration.createUnsafeBinaryConfiguration();

    private final TreeMap<Long, Disk> hashCircle = new TreeMap<>();

    private Node localNode;
    private List<Node> allNodes;
    private PersistentNode persistentNode;
    private PathChildrenCache pathChildrenCache;

    private Configuration configuration;

    @Autowired
    public DistributedTopology(Configuration configuration) {
        this.configuration = configuration;
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
        stopWatch();
        unregister();
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        // initialize local information
        initialize();

        // register node to zookeeper
        register();

        // start watch nodes
        startWatch();
    }

    private void initialize() throws IOException {
        Node node = initLocalNode();
        List<Disk> disks = configuration.getDisks();
        node.setDisks(disks);
        for (Disk disk : disks) {
            disk.setNode(node);
            if (disk.getWeight() <= 0) {
                disk.setWeight(configuration.getDiskDefaultWeight());
            }
        }
        this.localNode = node;
    }

    private void register() {
        Node node = this.localNode;
        short id = node.getId();
        String path = ZKPaths.makePath(ZK_BASE_PATH, "nodes", String.valueOf(id));
        CuratorFramework client = configuration.getZookeeperClient();
        byte[] data = fst.asByteArray(node);
        this.persistentNode = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, data);
        this.persistentNode.start();
    }

    private void unregister() throws IOException {
        persistentNode.close();
    }

    private void startWatch() throws Exception {
        String path = ZKPaths.makePath(ZK_BASE_PATH, "nodes");
        pathChildrenCache = new PathChildrenCache(configuration.getZookeeperClient(), path, false);
        pathChildrenCache.getListenable().addListener((client, event) -> {

            List<Node> nodes = new ArrayList<>();
            for (String child : client.getChildren().forPath(path)) {
                String childPath = ZKPaths.makePath(path, child);
                byte[] data = client.getData().forPath(childPath);
                nodes.add((Node) fst.asObject(data));
            }

            Random random = new Random();
            // create router
            for (Node node : nodes) {
                for (Disk disk : node.getDisks()) {
                    int uniqueDiskId = NumericUtil.combineTwoShorts(node.getId(), disk.getId());
                    random.setSeed(uniqueDiskId);

                    int vnodeCount = (int) (disk.getWeight() * configuration.getVnodeFactor());
                    for (int i = 0; i < vnodeCount; i++) {
                        int vnodeId = random.nextInt();
                        long uniqueVNodeId = NumericUtil.combineTwoInts(vnodeId, uniqueDiskId);
                        hashCircle.put(uniqueVNodeId, disk);
                    }
                }
            }

            byte[] data = event.getData().getData();
            Node node = (Node) fst.asObject(data);
            LOG.info("{} {}", node.toString(), event.getType().name());
            switch (event.getType()) {
                case CHILD_ADDED:
                    break;
                case CHILD_UPDATED:
                    break;
                case CHILD_REMOVED:
                    break;
                case CONNECTION_LOST:
                    break;
                case CONNECTION_SUSPENDED:
                    break;
                case CONNECTION_RECONNECTED:
                    break;
                case INITIALIZED:
                    break;
                default:
                    // do nothing
                    break;
            }
        });
        pathChildrenCache.start();

    }

    private void stopWatch() throws IOException {
        pathChildrenCache.close();
    }

    private Node initLocalNode() throws IOException {
        Node node = new Node();
        node.setId(configuration.getId());
        node.setPort(configuration.getPort());
        InetAddress localHost = getLocalHostFromZK(configuration.getZookeeperQuorum());
        node.setHostAddress(localHost.getHostAddress());
        node.setHostName(localHost.getHostName());
        return node;
    }

    private Disk tryParseDisk(String key, String value) {
        String prefix = "ceresfs.disk.";
        if (!key.startsWith(prefix)) {
            return null;
        }
        Short id = tryParseShort(key.substring(prefix.length()));
        if (id == null) {
            return null;
        }
        String[] pathAndWeight = value.split(":");

        String path = pathAndWeight[0];
        double weight = pathAndWeight.length > 1 ?
                Double.parseDouble(pathAndWeight[1]) :
                configuration.getDiskDefaultWeight();
        return new Disk(id, path, weight);
    }

    private Short tryParseShort(String s) {
        Integer integer = Ints.tryParse(s);
        if (integer == null) {
            return null;
        }

        // FIXME
        return integer.shortValue();
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
