package com.supconit.ceresfs.topology;

import java.util.List;

public interface Topology {

    enum Mode {

        STANDALONE,

        DISTRIBUTED,
    }

    Mode mode();

    Node localNode();

    List<Node> allNodes();
   
    Disk route(byte[] id);
    
    Disk route(long id);
}
