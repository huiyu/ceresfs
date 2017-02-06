package com.supconit.ceresfs;

import org.apache.curator.utils.ZKPaths;

public final class Const {

    public static final String ZK_BASE_PATH = "/ceresfs";

    public static final String ZK_NODES_PATH = ZK_BASE_PATH + "/nodes";

    public static final String ZK_CONFIG_PATH = ZK_BASE_PATH + "/configuration";

    public static String makeZKNodePath(short nodeId) {
        return ZKPaths.makePath(ZK_NODES_PATH, String.valueOf(nodeId));
    }
}
