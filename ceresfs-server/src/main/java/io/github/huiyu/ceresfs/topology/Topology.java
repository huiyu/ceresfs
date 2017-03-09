package io.github.huiyu.ceresfs.topology;

import java.util.List;

public interface Topology {

    Node getLocalNode();
    
    boolean isLocalNode(Node node);

    List<Node> getAllNodes();

    List<Node> getUnbalancedNodes();

    Disk route(byte[] id);

    Disk route(long id);
}
