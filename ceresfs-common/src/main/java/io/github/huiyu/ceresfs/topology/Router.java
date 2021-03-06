package io.github.huiyu.ceresfs.topology;

import java.util.List;

public interface Router {

    Disk route(byte[] id);

    List<Disk> route(byte[] id, int replication);
}
