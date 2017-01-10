package com.supconit.ceresfs.topology;

import com.google.common.primitives.Longs;

import com.supconit.ceresfs.config.Configuration;
import com.supconit.ceresfs.util.Codec;
import com.supconit.ceresfs.util.NetUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanInstantiationException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class DistributedTopology implements Topology, InitializingBean, DisposableBean, ApplicationContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedTopology.class);

    private static final String ZK_BASE_PATH = "/ceresfs";

    private final List<TopologyChangeListener> topologyChangeListeners = new ArrayList<>();

    private final Configuration configuration;

    private volatile Node localNode;
    private volatile List<Node> allNodes;
    private volatile PersistentNode register;
    private volatile PathChildrenCache watcher;
    private volatile ConsistentHashing router;

    @Autowired
    public DistributedTopology(Configuration configuration) {
        this.configuration = configuration;
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
        return router.route(id);
    }

    @Override
    public Disk route(long id) {
        return router.route(Longs.toByteArray(id));
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

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        Map<String, TopologyChangeListener> listenerByName = ctx.getBeansOfType(TopologyChangeListener.class);
        if (listenerByName != null && !listenerByName.isEmpty())
            topologyChangeListeners.addAll(listenerByName.values());
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

    private void register() throws Exception {
        Node node = this.localNode;
        short id = node.getId();
        String path = ZKPaths.makePath(ZK_BASE_PATH, "nodes", String.valueOf(id));
        CuratorFramework client = configuration.getZookeeperClient();

        // check 
        if (client.checkExists().forPath(path) != null) {
            throw new BeanInstantiationException(this.getClass(),
                    "node[id=" + id + "] already exists");
        }

        byte[] data = Codec.encode(node);
        this.register = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, data);
        this.register.start();
    }

    private void unregister() throws IOException {
        register.close();
    }

    private void startWatch() throws Exception {
        String path = ZKPaths.makePath(ZK_BASE_PATH, "nodes");
        watcher = new PathChildrenCache(configuration.getZookeeperClient(), path, false);
        watcher.getListenable().addListener((client, event) -> {

            List<Node> nodes = new ArrayList<>();
            List<String> children = client.getChildren().forPath(path);
            for (String child : children) {
                String childPath = ZKPaths.makePath(path, child);
                byte[] data = client.getData().forPath(childPath);
                nodes.add((Node) Codec.decode(data));
            }
            allNodes = nodes;
            // build router
            router = new ConsistentHashing(nodes, configuration.getVnodeFactor());

            switch (event.getType()) {
                case CHILD_ADDED:
                    final Node nodeAdd = (Node) Codec.decode(client.getData().forPath(event.getData().getPath()));
                    LOG.info("{} added", nodeAdd.toString());

                    // ignore local node
                    if (nodeAdd.equals(localNode())) {
                        break;
                    }
                    // invoke listener#onNodeAdd
                    topologyChangeListeners.forEach(listener -> {
                        try {
                            listener.onNodeAdded(nodeAdd);
                        } catch (Exception e) {
                            LOG.error("Invoke " + listener.getClass().getName() + "#onNodeAdded error", e);
                        }
                    });
                    break;
                case CHILD_REMOVED:
                    final Node nodeRemoved = (Node) Codec.decode(client.getData().forPath(event.getData().getPath()));
                    LOG.info("{} removed", nodeRemoved.toString());

                    // ignore local node
                    if (nodeRemoved.equals(localNode())) {
                        break;
                    }

                    // invoke listener#onNodeRemoved
                    topologyChangeListeners.forEach(listener -> {
                        try {
                            listener.onNodeRemoved(nodeRemoved);
                        } catch (Exception e) {
                            LOG.error("Invoke " + listener.getClass().getName() + "#onNodeRemoved error", e);
                        }
                    });
                    break;
                case CHILD_UPDATED:
                    Node nodeUpdated = (Node) Codec.decode(client.getData().forPath(event.getData().getPath()));
                    LOG.info("{} updated", nodeUpdated.toString());

                    break;
                default:
                    break;
            }
        });
        watcher.start();
    }

    private void stopWatch() throws IOException {
        watcher.close();
    }

    private Node initLocalNode() throws IOException {
        Node node = new Node();
        node.setId(configuration.getId());
        node.setPort(configuration.getPort());
        InetAddress localHost = getLocalHostFromZK(configuration.getZookeeperAddress());
        node.setHostAddress(localHost.getHostAddress());
        node.setHostName(localHost.getHostName());
        return node;
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
