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
    public void testBasic() throws IOException {
        long volumeFileName = System.currentTimeMillis();
        File file = new File(folder.getRoot(), String.valueOf(volumeFileName));
        long time = System.currentTimeMillis();
        try (VolumeFile.Writer writer = VolumeFile.createWriter(file)) {
            Image.Index index = new Image.Index();
            index.setId(10001L);
            index.setVolume(volumeFileName);
            index.setFlag(Image.FLAG_NORMAL);
            index.setOffset(0L);
            index.setSize(100);
            index.setType(Image.Type.JPG);
            index.setTime(time);
            index.setExpireTime(-1L);
            byte[] data = new byte[100];
            Image image = new Image(index, data);
            writer.write(image);
            writer.flush();

            writer.write(image);
            assertEquals(164L, image.getIndex().getOffset());
        }

        try (VolumeFile.Reader reader = VolumeFile.createReader(file)) {
            Image image = reader.readImage();
            Image.Index index = image.getIndex();
            assertEquals(10001L, index.getId());
            assertEquals(volumeFileName, index.getVolume());
            assertEquals(Image.FLAG_NORMAL, index.getFlag());
            assertEquals(0L, index.getOffset());
            assertEquals(100, index.getSize());
            assertEquals(Image.Type.JPG, index.getType());
            assertEquals(time, index.getTime());
            assertEquals(-1, index.getExpireTime());
            assertArrayEquals(new byte[100], image.getData());

            reader.seek(164);
            image = reader.readImage();
            assertNotNull(image);
            
            reader.seek(0L);
            image = reader.readImage();
            assertNotNull(image);
        }
    }
}