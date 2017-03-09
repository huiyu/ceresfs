package io.github.huiyu.ceresfs.topology;

import com.google.common.primitives.Longs;

import io.github.huiyu.ceresfs.config.Configuration;
import io.github.huiyu.ceresfs.http.HttpClientPool;
import io.github.huiyu.ceresfs.storage.Balancer;
import io.github.huiyu.ceresfs.storage.DelayedBalancer;
import io.github.huiyu.ceresfs.storage.Directory;
import io.github.huiyu.ceresfs.storage.Store;
import io.github.huiyu.ceresfs.util.Codec;
import io.github.huiyu.ceresfs.util.NetUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanInstantiationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class DistributedTopology implements Topology, InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedTopology.class);

    private static final String ZK_BASE_PATH = "/ceresfs";

    private volatile Node localNode;
    private volatile PersistentNode register;

    private ListenableRouter router;

    private volatile Map<Short, Node> unbalanced;

    private final Configuration config;
    private final Balancer balancer;


    public DistributedTopology(Configuration config, Balancer balancer) {
        this.config = config;
        this.balancer = balancer;
    }

    @Autowired
    public DistributedTopology(Configuration config,
                               HttpClientPool httpClientPool,
                               Directory directory,
                               Store store
    ) {
        this.config = config;
        this.balancer = new DelayedBalancer(this, httpClientPool, directory, store);
    }

    @Override
    public Node getLocalNode() {
        return localNode;
    }

    @Override
    public boolean isLocalNode(Node node) {
        return localNode.getId() == node.getId();
    }

    @Override
    public List<Node> getAllNodes() {
        return router.getNodes();
    }

    @Override
    public List<Node> getUnbalancedNodes() {
        return new ArrayList<>(unbalanced.values());
    }

    @Override
    public Disk route(byte[] id) {
        return router.route(id);
    }

    @Override
    public Disk route(long id) {
        return router.route(Longs.toByteArray(id));
    }

    public void startBalancer() {
        if (balancer.isRunning()) {
            stopBalancer();
        }
        balancer.start(config.getBalanceDelay(), config.getBalanceDelayTimeUnit())
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        Node node = this.localNode;
                        node.setBalanced(true);
                        try {
                            register.setData(Codec.encode(node));
                        } catch (Exception e) {
                            // FIXME
                            ex = e;
                        }
                    }
                    if (ex != null) {
                        LOG.error("Balance error ", ex);
                    }
                });
    }

    public void stopBalancer() {
        balancer.cancel();
    }

    @Override
    public void destroy() throws Exception {
        if (balancer.isRunning()) {
            balancer.cancel();
        }
        stopRouter();
        unregister();
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        // initialize local information
        initialize();

        // register node to zookeeper
        register();

        // start router
        startRouter();

        // start balance at startup
        startBalancer();
    }

    private void startRouter() throws Exception {
        router = new ListenableRouter(config.getZookeeperClient(), config.getVnodeFactor());
        router.addTopologyChangeListener(new TopologyChangeListener() {

            @Override
            public void onNodeAdded(Node node) {
                startBalancer();
            }

            @Override
            public void onNodeRemoved(Node node) {
                startBalancer();
            }

            @Override
            public void onDiskAdded(Disk disk) {
                startBalancer();
            }

            @Override
            public void onDiskRemoved(Disk disk) {
                startBalancer();

            }

            @Override
            public void onDiskWeightChanged(Disk disk, double newWeight) {
                startBalancer();
            }

            @Override
            public void onNodeBalanceChanged(Node node) {
                if (node.isBalanced()) {
                    unbalanced.remove(node.getId());
                } else {
                    unbalanced.put(node.getId(), node);
                }
            }
        });
        this.unbalanced = router.getNodes().stream()
                .filter(node -> !node.isBalanced())
                .collect(Collectors.toMap(Node::getId, Function.identity()));
    }

    private void stopRouter() throws IOException {
        router.close();
    }

    private void initialize() throws Exception {
        Node node = initLocalNode();
        List<Disk> disks = config.getDisks();
        node.setDisks(disks);
        for (Disk disk : disks) {
            disk.setNode(node);
            if (disk.getWeight() <= 0) {
                disk.setWeight(config.getDiskDefaultWeight());
            }
        }
        this.localNode = node;
    }

    private void register() throws Exception {
        Node node = this.localNode;
        short id = node.getId();
        String path = ZKPaths.makePath(ZK_BASE_PATH, "nodes", String.valueOf(id));
        CuratorFramework client = config.getZookeeperClient();

        // check 
        if (client.checkExists().forPath(path) != null) {
            throw new BeanInstantiationException(this.getClass(),
                    "node[id=" + id + "] already exists");
        }

        byte[] data = Codec.encode(node);
        this.register = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, data);
        this.register.start();
        this.register.waitForInitialCreate(5, TimeUnit.MINUTES);
    }

    private void unregister() throws IOException {
        register.close();
    }

    private Node initLocalNode() throws Exception {
        Node node = new Node();
        node.setBalanced(false);
        node.setId(config.getId());
        node.setPort(config.getPort());
        InetAddress localHost = NetUtil.getLocalAddressFromZookeeper(config.getZookeeperClient());
        node.setHostAddress(localHost.getHostAddress());
        node.setHostName(localHost.getHostName());
        return node;
    }
}
