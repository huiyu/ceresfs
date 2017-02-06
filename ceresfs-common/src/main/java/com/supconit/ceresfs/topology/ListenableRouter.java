package com.supconit.ceresfs.topology;

import com.supconit.ceresfs.Const;
import com.supconit.ceresfs.util.Codec;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class ListenableRouter implements Router, Closeable {

    public static final Logger LOG = LoggerFactory.getLogger(ListenableRouter.class);

    private PathChildrenCache cache;
    private ConsistentHashingRouter router;

    private final Set<TopologyChangeListener> listeners = new HashSet<>();
    private final ListenerAgency listenerAgency = new ListenerAgency();
    private final ReentrantLock lock = new ReentrantLock();

    private final BiFunction<List<Disk>, Disk, Boolean> contains = (disks, disk) -> {
        for (Disk member : disks) {
            if (member.getId() == disk.getId())
                return true;
        }
        return false;
    };

    public ListenableRouter(CuratorFramework client, int vnodeFactor) throws Exception {
        String path = Const.ZK_NODES_PATH;
        List<Node> nodes = new ArrayList<>();
        List<String> children = client.getChildren().forPath(path);

        for (String child : children) {
            String childPath = ZKPaths.makePath(path, child);
            byte[] data = client.getData().forPath(childPath);
            nodes.add((Node) Codec.decode(data));
        }

        // generate watcher
        this.router = new ConsistentHashingRouter(nodes, vnodeFactor);

        this.cache = new PathChildrenCache(client, path, false);
        this.cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        initialize();
    }

    private void initialize() {
        this.cache.getListenable().addListener((client, event) -> {
            lock.lock();
            try {
                Callable<Node> getNodeFromZK = () ->
                        ((Node) Codec.decode(client.getData().forPath(event.getData().getPath())));
                Callable<Node> getNodeFromLocal = () -> {
                    String p = event.getData().getPath();
                    short nodeId = Short.valueOf(p.substring(p.lastIndexOf("/") + 1, p.length()));
                    return router.getNode(nodeId);
                };

                // handle topology changed
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        Node node = getNodeFromZK.call();
                        listenerAgency.onNodeAdded(node);
                        break;
                    }
                    case CHILD_REMOVED: {
                        Node node = getNodeFromLocal.call();
                        listenerAgency.onNodeRemoved(node);
                        break;
                    }
                    case CHILD_UPDATED: {
                        Node original = getNodeFromLocal.call();
                        Node updated = getNodeFromZK.call();

                        LOG.info("{} updated", updated.toString());

                        // handle disk added
                        List<Disk> nodesToAdd = updated.getDisks().stream()
                                .filter(disk -> !contains.apply(original.getDisks(), disk))
                                .collect(Collectors.toList());
                        for (Disk disk : nodesToAdd) {
                            listenerAgency.onDiskAdded(disk);
                        }

                        // handle disk removed
                        List<Disk> nodesToRemove = original.getDisks().stream()
                                .filter(disk -> !contains.apply(updated.getDisks(), disk))
                                .collect(Collectors.toList());

                        for (Disk disk : nodesToRemove) {
                            listenerAgency.onDiskRemoved(disk);
                        }


                        // handle disk weight changed
                        final double illegalWeight = 0.0;
                        original.getDisks().stream()
                                .collect(Collectors.toMap(Function.identity(), disk -> {
                                    for (Disk d : updated.getDisks()) {
                                        if (d.getId() == disk.getId()
                                                && d.getWeight() != disk.getWeight())
                                            return disk.getWeight();
                                    }
                                    return illegalWeight;
                                }))
                                .forEach((disk, weight) -> {
                                    if (weight > illegalWeight) {
                                        listenerAgency.onDiskWeightChanged(disk, weight);
                                    }
                                });

                        // handle node balance status
                        boolean balanceChanged = (original.isBalanced() != updated.isBalanced());
                        if (balanceChanged) {
                            Node node = router.getNode(updated.getId());
                            node.setBalanced(updated.isBalanced());
                            listenerAgency.onNodeBalanceChanged(node);

                        }
                        break;
                    }
                    default: {
                        // ignore other events.
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        });
    }

    public void addTopologyChangeListener(TopologyChangeListener listener) {
        lock.lock();
        try {
            listeners.add(checkNotNull(listener));
        } finally {
            lock.unlock();
        }
    }

    public void removeTopologyChangeListener(TopologyChangeListener listener) {
        try {
            listeners.remove(checkNotNull(listener));
        } finally {
            lock.unlock();
        }
    }

    public List<Node> getNodes() {
        return router.getNodes();
    }

    @Override
    public void close() throws IOException {
        this.cache.close();
    }

    @Override
    public Disk route(byte[] id) {
        return router.route(id);
    }

    @Override
    public List<Disk> route(byte[] id, int replication) {
        return router.route(id, replication);
    }

    private class ListenerAgency implements TopologyChangeListener {

        @Override
        public void onNodeAdded(Node node) {
            LOG.info("{} added", node.toString());
            for (TopologyChangeListener listener : listeners) {
                try {
                    listener.onNodeAdded(node);
                } catch (Throwable e) {
                    LOG.error(listener.getClass().getName() +
                            "#onNodeAdded invoked error", e);
                }
            }
            router.addNode(node);
        }

        @Override
        public void onNodeRemoved(Node node) {
            LOG.info("{} removed", node.toString());
            for (TopologyChangeListener listener : listeners) {
                try {
                    listener.onNodeRemoved(node);
                } catch (Throwable e) {
                    LOG.error(listener.getClass().getName() + "#onNodeRemoved invoked error", e);
                }
            }
            router.removeNode(node);
        }

        @Override
        public void onDiskAdded(Disk disk) {
            for (TopologyChangeListener listener : listeners) {
                try {
                    listener.onDiskAdded(disk);
                } catch (Throwable e) {
                    LOG.error(listener.getClass().getName() + "#onDiskAdded invoked error", e);
                }
            }
            router.addDisk(disk);
        }

        @Override
        public void onDiskRemoved(Disk disk) {
            for (TopologyChangeListener listener : listeners) {
                try {
                    listener.onDiskRemoved(disk);
                } catch (Throwable e) {
                    LOG.error(listener.getClass().getName() + "#onDiskRemoved invoked error", e);
                }
            }
            router.removeDisk(disk);
        }

        @Override
        public void onDiskWeightChanged(Disk disk, double newWeight) {
            router.changeDiskWeight(disk, newWeight);
            for (TopologyChangeListener listener : listeners) {
                try {
                    listener.onDiskWeightChanged(disk, newWeight);
                } catch (Throwable e) {
                    LOG.error(listener.getClass().getName() + "#onDiskWeightChanged invoked error", e);
                }
            }
        }

        @Override
        public void onNodeBalanceChanged(Node node) {
            for (TopologyChangeListener listener : listeners) {
                listener.onNodeBalanceChanged(node);
            }
        }
    }
}
