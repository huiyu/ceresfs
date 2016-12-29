package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.topology.Disk;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class VolumeImageStoreTest {

    private static final long SIZE_1MB = 1024L * 1024L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File volumeDir;

    @Before
    public void setUp() throws Exception {
        volumeDir = folder.newFolder();

    }

    @Test
    public void test() throws Exception {
        Disk disk = new Disk(volumeDir.getPath(), 1.0);
        VolumeImageStore store = new VolumeImageStore(SIZE_1MB);

        store.prepareSave(disk, 1L, Image.Type.JPG, new byte[(int) (SIZE_1MB / 2L)])
                .onSuccess(image -> {
                    assertEquals(1L, image.getIndex().getId());
                })
                .save(true);
        // automatic create new volume file
        store.prepareSave(disk, 1L, Image.Type.JPG, new byte[(int) (SIZE_1MB / 2L)]).save(true);
        for (File file : volumeDir.listFiles()) {
            System.out.println(file.length());
        }
        assertEquals(2, volumeDir.listFiles().length);
        store.destroy();
    }
}
