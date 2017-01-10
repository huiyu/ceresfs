package com.supconit.ceresfs.util;

import org.nustaq.serialization.FSTConfiguration;

public class Codec {

    private static final FSTConfiguration FST = FSTConfiguration.createDefaultConfiguration();

    public static byte[] encode(Object o) {
        return FST.asByteArray(o);
    }

    public static Object decode(byte[] b) {
        return FST.asObject(b);
    }
}
