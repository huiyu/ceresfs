package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.ImageType;
import com.supconit.ceresfs.config.Configuration;
import com.supconit.ceresfs.topology.Disk;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class VolumeStoreTest {

    private static final long SIZE_1MB = 1024L * 1024L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    VolumeStore store;

    private File volumeDir;

    @Before
    public void setUp() throws Exception {
        volumeDir = folder.newFolder();

        Configuration config = mock(Configuration.class);
        when(config.getVolumeMaxSize()).thenReturn(SIZE_1MB);
        when(config.getVolumeWriteParallelism()).thenReturn(4);
        PooledVolumeContainer volumeContainer = new PooledVolumeContainer(config);
        store = new VolumeStore(volumeContainer);
    }

    @After
    public void tearDown() throws Exception {
        folder.delete();

    }

    @Test
    public void testAutoCreateNewVolume() throws Exception {
        Disk disk = new Disk();
        disk.setPath(volumeDir.getPath());
        disk.setWeight(1.0);

        store.save(disk, 1L, ImageType.JPG, new byte[(int) (SIZE_1MB / 2L)]).get();
        store.save(disk, 1L, ImageType.JPG, new byte[(int) (SIZE_1MB / 2L)]).get();

        // automatic create new volume file
        store.save(disk, 1L, ImageType.JPG, new byte[(int) (SIZE_1MB / 2L)]).get();
        assertEquals(2,
                Stream.of(volumeDir.listFiles()).filter(file -> file.length() > 0).count());
        store.destroy();
    }
}
