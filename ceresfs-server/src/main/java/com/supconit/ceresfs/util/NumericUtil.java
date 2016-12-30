package com.supconit.ceresfs.util;

public class NumericUtil {

    public static int combineTwoShorts(short x, short y) {
        return ((int) x) << 16 | y & 0xffff;
    }

    public static long combineTwoInts(int x, int y) {
        return (((long) x) << 32) | (y & 0xffffffffL);
    }
}
