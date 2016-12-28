package com.supconit.ceresfs.topology;

import org.nustaq.serialization.FSTConfiguration;
import org.springframework.stereotype.Component;

@Component
public class DefaultNodeCodec implements NodeCodec {

    /**
     * FIXME: 10000 vnodes cost about 300KB memory, which ZNode has 1MB size limit.
     */
    private FSTConfiguration fst = FSTConfiguration.createUnsafeBinaryConfiguration();

    @Override
    public byte[] encode(Node node) {
        for (Disk disk : node.getDisks()) {
            disk.setNode(null);
            for (VNode vnode : disk.getVnodes()) {
                vnode.setDisk(null);
            }
        }
        return fst.asByteArray(node);
    }

    @Override
    public Node decode(byte[] data) {
        Node node = (Node) fst.asObject(data);
        for (Disk disk : node.getDisks()) {
            disk.setNode(node);
            for (VNode vnode : disk.getVnodes()) {
                vnode.setDisk(disk);
            }
        }
        return node;
    }
}
