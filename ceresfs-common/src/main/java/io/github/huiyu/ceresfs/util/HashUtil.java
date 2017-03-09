package io.github.huiyu.ceresfs.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashUtil {

    private final static HashFunction MURMUR = Hashing.murmur3_128(0);

    public static long murmur(byte[] key) {
        return MURMUR.hashBytes(key).asLong();
    }
}
