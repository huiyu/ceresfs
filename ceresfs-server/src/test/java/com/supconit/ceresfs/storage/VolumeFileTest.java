package com.supconit.ceresfs.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class VolumeFileTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Before
    public void setup() throws IOException {
        folder.create();
    }

    @Test
    public void test() throws IOException {
        long currentTime = System.currentTimeMillis();
        File file = new File(folder.getRoot(), String.valueOf(currentTime));
        // write
        try (VolumeFile.Writer writer = VolumeFile.createWriter(file)) {
            Image.Index index = new Image.Index();
            index.setId(1L);
            index.setFlag(Image.FLAG_NORMAL);
            index.setType(Image.Type.JPG);
            index.setExpireTime(-1L);
            byte[] data = new byte[100];
            Image image = new Image(index, data);
            writer.write(image);
            writer.flush();

            assertEquals(currentTime, index.getVolume());
            assertEquals(0L, image.getIndex().getOffset());
            assertTrue(image.getIndex().getTime() > currentTime);

            image.getIndex().setId(2L);
            writer.write(image);
            writer.flush();
            assertEquals(currentTime, index.getVolume());
            assertEquals(164L, image.getIndex().getOffset());
            assertTrue(image.getIndex().getTime() > currentTime);
        }

        // read
        try (VolumeFile.Reader reader = VolumeFile.createReader(file)) {
            Image image = reader.read(0);
            assertEquals(1L, image.getIndex().getId());
            assertArrayEquals(new byte[100], image.getData());
            image = reader.read(164);
            assertEquals(2L, image.getIndex().getId());
            assertArrayEquals(new byte[100], image.getData());
        }

        // iterate
        try (VolumeFile.Reader reader = VolumeFile.createReader(file)) {
            Image next;
            int count = 0;
            while ((next = reader.next()) != null) {
                assertArrayEquals(new byte[100], next.getData());
                assertEquals(-1L, next.getIndex().getExpireTime());
                count++;
            }
            assertEquals(2, count);
        }
    }
}
    
