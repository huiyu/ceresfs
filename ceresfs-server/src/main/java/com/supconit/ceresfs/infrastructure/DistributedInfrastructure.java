package com.supconit.ceresfs.infrastructure;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(prefix = "zookeeper", name = "quorum")
public class DistributedInfrastructure implements Infrastructure, InitializingBean, DisposableBean {

    private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${port:9900}")
    private int port;
    @Value("${directories:/tmp:1}")
    private String directories;
    @Value("${zookeeper.quorum}")
    private String zkQuorum;
    @Value("${zookeeper.basePath:/ceresfs}")
    private String zkBasePath;

    private Host localHost;
    private List<Host> hosts;

    private CuratorFramework zkClient;
    private PersistentNode node;
    private PathChildrenCache cache;

    @Override
    public Mode getMode() {
        return Mode.DISTRIBUTED;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        zkClient = CuratorFrameworkFactory.newClient(zkQuorum, new ExponentialBackoffRetry(200, 10));
        zkClient.start();
        initLocalHost();
        registerHost();
        watchHosts();
    }

    private void initLocalHost() throws IOException {
        for (String s : this.zkQuorum.split(";")) {
            String[] ipPort = s.split(":");
            String ip = ipPort[0];
            int port = ipPort.length > 1 ? Integer.parseInt(ipPort[1]) : 2181;
            try (Socket socket = new Socket(ip, port)) {
                InetAddress localhost = socket.getLocalAddress();
                Host host = new Host();
                host.setName(localhost.getHostName());
                host.setAddress(localhost.getHostAddress());
                host.setPort(this.port);
                Map<String, String> weightByPath
                        = Splitter.on(";").trimResults().withKeyValueSeparator(":").split(this.directories);
                List<Directory> directories = weightByPath.keySet().stream().map(path -> {
                    String weight = weightByPath.get(path);
                    return new Directory(path, StringUtils.isEmpty(weight) ? 1.0 : Double.parseDouble(weight));
                }).collect(Collectors.toList());

                host.setDirectories(directories);
                this.localHost = host;
                return;
            } catch (ConnectException e) {
                // DO NOTHING
            }
        }
        throw new ConnectException("Connection refused: " + zkClient);
    }

    private void registerHost() throws Exception {
        String identify = getLocalHost().getAddress() + ":" + getLocalHost().getPort();
        String path = ZKPaths.makePath(zkBasePath, "hosts", identify);
        byte[] data = JSON.toJSONString(this.getLocalHost()).getBytes(DEFAULT_CHARSET);
        node = new PersistentNode(zkClient, CreateMode.EPHEMERAL, false, path, data);
        node.start();
    }

    private void watchHosts() throws Exception {
        cache = new PathChildrenCache(zkClient, ZKPaths.makePath(zkBasePath, "hosts"), true);
        cache.getListenable().addListener((client, event) -> {
            this.hosts = client.getChildren()
                               .forPath(ZKPaths.makePath(zkBasePath, "hosts"))
                               .stream()
                               .map(child -> {
                                   try {
                                       String path = ZKPaths.makePath(zkBasePath, "hosts", child);
                                       byte[] data = client.getData().forPath(path);
                                       String text = new String(data, DEFAULT_CHARSET);
                                       return JSON.parseObject(text, Host.class);
                                   } catch (Exception e) {
                                       logger.error("Can't get data from zookeeper path " + child, e);
                                       return null;
                                   }
                               })
                               .filter(Objects::nonNull)
                               .collect(Collectors.toList());
        });
        cache.start();
    }

    @Override
    public void destroy() throws Exception {
        cache.close();
        node.close();
        zkClient.close();
    }

    public List<Host> getAllHosts() {
        return hosts;
    }

    @Override
    public Host getLocalHost() {
        return localHost;
    }
}
