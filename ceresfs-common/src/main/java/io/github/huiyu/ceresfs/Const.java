package io.github.huiyu.ceresfs;

import org.apache.curator.utils.ZKPaths;

import io.netty.handler.codec.http.HttpHeaderNames;

public final class Const {

    public static final String ZK_BASE_PATH = "/ceresfs";

    public static final String ZK_NODES_PATH = ZK_BASE_PATH + "/nodes";

    public static final String ZK_CONFIG_PATH = ZK_BASE_PATH + "/configuration";

    public static final int MAX_IMAGE_SIZE = 128 * 1024 * 1024;

    public static final String HTTP_TOKEN_NAME = "token";

    public static final String HTTP_HEADER_IMAGE_ID = "id";

    public static final String HTTP_HEADER_EXPIRE_TIME = "expire-time";
    
    public static final String HTTP_HEADER_REPLICATION = "replication";

    public static final CharSequence HTTP_HEADER_MAX_FORWARDS = HttpHeaderNames.MAX_FORWARDS;

    public static final CharSequence HTTP_HEADER_CONTENT_TYPE = HttpHeaderNames.CONTENT_TYPE;

    public static String makeZKNodePath(short nodeId) {
        return ZKPaths.makePath(ZK_NODES_PATH, String.valueOf(nodeId));
    }
}
