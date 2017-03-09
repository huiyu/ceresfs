package io.github.huiyu.ceresfs.util;

public class NumericUtil {

    public static int combineTwoShorts(short x, short y) {
        return ((int) x) << 16 | y & 0xffff;
    }

    public static int highOfLong(long x) {
        return (int) (x >> 32);
    }

    public static int lowOfLong(long x) {
        return ((int) x);
    }

    public static long combineTwoInts(int x, int y) {
        return (((long) x) << 32) | (y & 0xffffffffL);
    }

    public static short highOfInt(int x) {
        return ((short) (x >> 16));
    }

    public static short lowOfInt(int x) {
        return ((short) x);
    }
}
