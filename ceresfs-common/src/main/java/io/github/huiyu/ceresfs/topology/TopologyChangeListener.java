package io.github.huiyu.ceresfs.topology;

public interface TopologyChangeListener {

    void onNodeAdded(Node node);

    void onNodeRemoved(Node node);

    void onDiskAdded(Disk disk);

    void onDiskRemoved(Disk disk);

    void onDiskWeightChanged(Disk disk, double newWeight);

    void onNodeBalanceChanged(Node node);
}
