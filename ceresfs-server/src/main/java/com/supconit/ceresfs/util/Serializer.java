package com.supconit.ceresfs.util;

public interface Serializer<T> {

    byte[] encode(T object);

    T decode(byte[] data);
}
