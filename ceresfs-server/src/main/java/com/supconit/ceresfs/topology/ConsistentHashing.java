package com.supconit.ceresfs.topology;

import com.supconit.ceresfs.util.HashUtil;
import com.supconit.ceresfs.util.NumericUtil;

import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class ConsistentHashing {

    private final Random random = new Random();
    private final List<Node> nodes;
    private final TreeMap<Long, Disk> hashCircle = new TreeMap<>();

    public ConsistentHashing(List<Node> nodes, int vnodeFactor) {
        this.nodes = nodes;
        Assert.notEmpty(nodes);
        for (Node node : nodes) {
            List<Disk> disks = node.getDisks();
            Assert.notEmpty(nodes);
            for (Disk disk : disks) {
                int uniqueDiskId = NumericUtil.combineTwoShorts(node.getId(), disk.getId());
                random.setSeed(uniqueDiskId);
                // Virtual node number
                int vnodeCount = (int) (disk.getWeight() * vnodeFactor);
                for (int i = 0; i < vnodeCount; i++) {
                    int vnodeId = random.nextInt();
                    long uniqueVNodeId = NumericUtil.combineTwoInts(vnodeId, uniqueDiskId);
                    hashCircle.put(uniqueVNodeId, disk);
                }
            }
        }
    }

    protected List<Node> getNodes() {
        return nodes;
    }

    public Disk route(byte[] id) {
        long hash = HashUtil.murmur(id);
        return ceiling(hash).getValue();
    }


    public List<Disk> route(byte[] id, int replication) {
        long hash = HashUtil.murmur(id);
        Map.Entry<Long, Disk> firstEntry = ceiling(hash);

        int count = nodes.size() < replication ? nodes.size() : replication;
        List<Disk> disks = new ArrayList<>(count);
        disks.add(firstEntry.getValue());
        // for checking duplicated node
        Set<Short> nodeIds = new HashSet<>(replication);
        nodeIds.add(firstEntry.getValue().getNode().getId());

        long currentKey = firstEntry.getKey();
        for (int i = 0; i < count - 1; i++) {
            // find next disk with different node
            while (true) {
                Map.Entry<Long, Disk> next = higher(currentKey);
                currentKey = next.getKey();
                Disk disk = next.getValue();
                short nodeId = disk.getNode().getId();
                if (!nodeIds.contains(nodeId)) {
                    disks.add(disk);
                    nodeIds.add(nodeId);
                    break;
                }
            }
        }
        return disks;
    }

    private Map.Entry<Long, Disk> ceiling(long hash) {
        Map.Entry<Long, Disk> ceiling = hashCircle.ceilingEntry(hash);
        if (ceiling == null) {
            return hashCircle.firstEntry();
        } else {
            return ceiling;
        }
    }

    private Map.Entry<Long, Disk> higher(long hash) {
        Map.Entry<Long, Disk> higher = hashCircle.higherEntry(hash);
        if (higher == null) {
            return hashCircle.firstEntry();
        } else {
            return higher;
        }
    }

}
