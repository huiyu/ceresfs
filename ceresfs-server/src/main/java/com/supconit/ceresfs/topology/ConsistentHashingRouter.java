package com.supconit.ceresfs.topology;

import com.supconit.ceresfs.util.HashUtil;
import com.supconit.ceresfs.util.NumericUtil;

import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashingRouter {

    private Random random = new Random();

    private final TreeMap<Long, Disk> hashCircle = new TreeMap<>();

    public ConsistentHashingRouter(List<Node> nodes, int vnodeFactor) {
        for (Node node : nodes) {
            for (Disk disk : node.getDisks()) {
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

    public Disk route(byte[] id) {
        SortedMap<Long, Disk> tail = hashCircle.tailMap(HashUtil.murmur(id));
        if (tail.size() == 0) {
            return hashCircle.get(hashCircle.firstKey());
        } else {
            return tail.get(tail.firstKey());
        }
    }
}
