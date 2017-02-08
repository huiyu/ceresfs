package com.supconit.ceresfs.client;

import com.supconit.ceresfs.Const;
import com.supconit.ceresfs.topology.ConsistentHashingRouter;
import com.supconit.ceresfs.topology.Node;
import com.supconit.ceresfs.util.Codec;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CeresFSClient implements Closeable {

    private CuratorFramework zookeeperClient;
    private PersistentNode node;
    private ConsistentHashingRouter router;

    public CeresFSClient(String zookeeperAddress) throws Exception {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(200, 10);
        zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperAddress, retryPolicy);
        zookeeperClient.start();

        String path = Const.ZK_NODES_PATH;

        List<Node> nodes = new ArrayList<>();
        List<String> children = zookeeperClient.getChildren().forPath(path);

        for (String child : children) {
            String childPath = ZKPaths.makePath(path, child);
            byte[] data = zookeeperClient.getData().forPath(childPath);
            nodes.add(((Node) Codec.decode(data)));
        }
    }

    public Image get(long id) {
        // TODO
        return null;
    }

    public CompletableFuture<Void> save(Image image) {
        // TODO
        return null;
    }

    public void delete(long id) {
        // TODO
    }

    @Override
    public void close() throws IOException {
        zookeeperClient.close();
    }
}