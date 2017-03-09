package io.github.huiyu.ceresfs.topology;

import io.github.huiyu.ceresfs.util.HashUtil;
import io.github.huiyu.ceresfs.util.NumericUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.*;

public class ConsistentHashingRouter implements Router {

    private final Random random = new Random();
    private final Map<Short, Node> nodes = new ConcurrentHashMap<>();
    private final TreeMap<Long, Disk> hashCircle = new TreeMap<>();
    private final int vnodeFactor;

    public ConsistentHashingRouter(int vnodeFactor) {
        this.vnodeFactor = vnodeFactor;
    }

    protected ConsistentHashingRouter(List<Node> nodes, int vnodeFactor) {
        this.vnodeFactor = vnodeFactor;
        checkNotNull(nodes);
        for (Node node : nodes) {
            addNode(node);
        }
    }

    protected List<Node> getNodes() {
        return new ArrayList<>(nodes.values());
    }

    protected Node getNode(short nodeId) {
        return nodes.get(nodeId);
    }

    protected Disk getDisk(short nodeId, short diskId) {
        Node node = getNode(nodeId);
        if (node == null)
            return null;

        for (Disk disk : node.getDisks()) {
            if (disk.getId() == diskId)
                return disk;
        }
        return null;
    }

    protected synchronized void addNode(Node node) {
        checkArgument(!nodes.containsKey(node.getId()));
        nodes.put(node.getId(), node);
        List<Disk> disks = node.getDisks();
        for (Disk disk : disks) {
            addDiskInternal(disk);
        }
    }

    protected synchronized void removeNode(Node node) {
        node = nodes.get(node.getId());
        nodes.remove(node.getId());
        List<Disk> disks = node.getDisks();
        for (Disk disk : disks) {
            removeDiskInternal(disk);
        }
    }

    protected synchronized void addDisk(Disk disk) {
        Node node = getNode(disk.getNode().getId());
        node.getDisks().add(disk);
        disk.setNode(node);
        addDiskInternal(disk);
    }

    private synchronized void addDiskInternal(Disk disk) {
        Node node = disk.getNode();
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

    protected synchronized void removeDisk(Disk disk) {
        Node node = getNode(disk.getNode().getId());
        Disk toDelete = null;
        for (Disk toCheck : node.getDisks()) {
            if (toCheck.getId() == disk.getId()) {
                toDelete = toCheck;
            }
        }

        if (toDelete != null) {
            node.getDisks().remove(toDelete);
            removeDiskInternal(toDelete);
        }
    }

    private synchronized void removeDiskInternal(Disk disk) {
        Node node = disk.getNode();
        int uniqueDiskId = NumericUtil.combineTwoShorts(node.getId(), disk.getId());
        random.setSeed(uniqueDiskId);
        int vnodeCount = (int) (disk.getWeight() * vnodeFactor);
        for (int i = 0; i < vnodeCount; i++) {
            int vnodeId = random.nextInt();
            long uniqueVNodeId = NumericUtil.combineTwoInts(vnodeId, uniqueDiskId);
            hashCircle.remove(uniqueVNodeId);
        }
    }

    protected synchronized void changeDiskWeight(Disk disk, double newWeight) {
        Disk original = getDisk(disk.getNode().getId(), disk.getId());
        checkNotNull(original);
        checkArgument(newWeight != original.getWeight());
        Disk updated = new Disk(original.getId(), original.getPath(), newWeight);
        updated.setNode(original.getNode());

        removeDisk(original);
        addDisk(disk);
    }

    @Override
    public synchronized Disk route(byte[] id) {
        long hash = HashUtil.murmur(id);
        return ceiling(hash).getValue();
    }

    @Override
    public synchronized List<Disk> route(byte[] id, int replication) {
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
