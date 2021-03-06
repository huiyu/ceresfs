package io.github.huiyu.ceresfs.storage;

import io.github.huiyu.ceresfs.ImageType;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class VolumeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Before
    public void setUp() throws IOException {
        folder.create();
    }

    @After
    public void tearDown() throws Exception {
        folder.delete();
    }

    @Test
    public void testReadAndWrite() throws IOException {
        long currentTime = System.currentTimeMillis();
        File file = new File(folder.getRoot(), String.valueOf(currentTime));
        // write
        try (Volume.Writer writer = Volume.createWriter(file)) {
            Image image = createImage(1L, ImageIndex.FLAG_NORMAL, ImageType.JPG, -1L, new byte[100]);
            writer.write(image);
            writer.flush();

            assertEquals(currentTime, image.getIndex().getVolume());
            assertEquals(0L, image.getIndex().getOffset());
            assertTrue(image.getIndex().getTime() >= currentTime);

            image.getIndex().setId(2L);
            writer.write(image);
            writer.flush();
            assertEquals(currentTime, image.getIndex().getVolume());
            assertEquals(164L, image.getIndex().getOffset());
            assertTrue(image.getIndex().getTime() >= currentTime);
        }

        // read
        try (Volume.Reader reader = Volume.createReader(file)) {
            Image image = reader.read(0);
            assertEquals(1L, image.getIndex().getId());
            assertArrayEquals(new byte[100], image.getData());
            image = reader.read(164);
            assertEquals(2L, image.getIndex().getId());
            assertArrayEquals(new byte[100], image.getData());
        }

        // iterate
        try (Volume.Reader reader = Volume.createReader(file)) {
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

    @Test
    public void testUpdate() throws IOException {
        long currentTime = System.currentTimeMillis();
        File file = new File(folder.getRoot(), String.valueOf(currentTime));
        try (Volume.Writer writer = Volume.createWriter(file)) {
            Image image = createImage(1L, ImageIndex.FLAG_NORMAL, ImageType.JPG, -1L, new byte[100]);
            writer.write(image);
        }

        try (Volume.Reader reader = Volume.createReader(file)) {
            Image image = reader.read(0);
            assertEquals(ImageIndex.FLAG_NORMAL, image.getIndex().getFlag());

            try (Volume.Writer writer = Volume.createWriter(file)) {
                writer.markDeleted(0);
                image = reader.read(0);
                assertEquals(ImageIndex.FLAG_DELETED, image.getIndex().getFlag());

                writer.markNormal(0);
                image = reader.read(0);
                assertEquals(ImageIndex.FLAG_NORMAL, image.getIndex().getFlag());
            }
        }
    }

    private Image createImage(long id, byte flag, ImageType type, long expireTime, byte[] data) {
        ImageIndex index = new ImageIndex();
        index.setId(id);
        index.setType(type);
        index.setExpireTime(expireTime);
        index.setFlag(flag);
        return new Image(index, data);
    }
}