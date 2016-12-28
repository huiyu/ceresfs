package com.supconit.ceresfs.topology;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConditionalOnProperty(prefix = "ceresfs", name = "mode", havingValue = "standalone")
public class StandaloneTopology implements Topology, InitializingBean {

    private List<Node> nodes;

    @Override
    public Mode mode() {
        return Mode.STANDALONE;
    }

    @Override
    public Node localNode() {
        return nodes.get(0);
    }

    @Override
    public List<Node> allNodes() {
        return nodes;
    }

    @Override
    public Disk route(byte[] id) {
        return null;
    }

    @Override
    public Disk route(long id) {
        return null;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        nodes = new ArrayList<>(1);
        // TODO create node
    }
}
