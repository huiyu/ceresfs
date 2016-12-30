package com.supconit.ceresfs.util;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.*;

public class NumericUtilTest {

    @Test
    public void testCombineTwoShorts() {
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            short x = (short) random.nextInt();
            short y = (short) random.nextInt();
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putShort(x).putShort(y).flip();
            int expected = buffer.getInt();
            assertEquals(expected, NumericUtil.combineTwoShorts(x, y));
        }
    }

    @Test
    public void testCombineTwoInts() {
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            int x = random.nextInt();
            int y = random.nextInt();
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(x).putInt(y).flip();
            long expected = buffer.getLong();
            assertEquals(expected, NumericUtil.combineTwoInts(x, y));
        }
    }
}