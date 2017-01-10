package com.supconit.ceresfs.topology;

import java.util.List;

public interface Topology {

    Node localNode();

    List<Node> allNodes();
 
    Disk route(byte[] id);

    Disk route(long id);
}
