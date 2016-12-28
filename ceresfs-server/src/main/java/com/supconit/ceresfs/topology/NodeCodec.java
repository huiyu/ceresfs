package com.supconit.ceresfs.topology;

public interface NodeCodec {

    byte[] encode(Node node);

    Node decode(byte[] data);
}
