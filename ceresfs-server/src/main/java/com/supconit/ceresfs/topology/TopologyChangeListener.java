package com.supconit.ceresfs.topology;

public interface TopologyChangeListener {

    void onNodeAdded(Node node);

    void onNodeRemoved(Node node);

    void onDiskAdded(Disk disk);

    void onDiskRemoved(Disk disk);

    void onDiskWeightChanged(Disk original, Disk present);
}
