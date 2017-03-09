package io.github.huiyu.ceresfs.compact;

import io.github.huiyu.ceresfs.ImageType;
import io.github.huiyu.ceresfs.config.Configuration;
import io.github.huiyu.ceresfs.storage.Directory;
import io.github.huiyu.ceresfs.storage.ImageIndex;
import io.github.huiyu.ceresfs.storage.PooledVolumeContainer;
import io.github.huiyu.ceresfs.storage.Store;
import io.github.huiyu.ceresfs.storage.Volume;
import io.github.huiyu.ceresfs.storage.VolumeContainer;
import io.github.huiyu.ceresfs.storage.VolumeStore;
import io.github.huiyu.ceresfs.topology.Disk;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MarkCopyVolumeCompactorTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        Configuration config = mock(Configuration.class);
        when(config.getVolumeMaxSize()).thenReturn(1024L);
        when(config.getVolumeCompactThreshold()).thenReturn(0.9);
        when(config.getVolumeCompactPeriod()).thenReturn(20L);
        when(config.getVolumeCompactPeriodTimeUnit()).thenReturn(TimeUnit.SECONDS);
    }

    @Test
    public void testIsExpired() throws Exception {
        MarkCopyVolumeCompactor compactor =
                new MarkCopyVolumeCompactor(null, null, null, null, null);
        long currentTime = System.currentTimeMillis();
        ImageIndex index = new ImageIndex();
        index.setExpireTime(0L);
        assertFalse(compactor.isExpired(currentTime, index));

        index.setExpireTime(currentTime - 10);
        assertTrue(compactor.isExpired(currentTime, index));

        index.setExpireTime(currentTime);
        assertFalse(compactor.isExpired(currentTime, index));

        index.setExpireTime(currentTime + 10L);
        assertFalse(compactor.isExpired(currentTime, index));
    }

    @Test
    public void testIsDeleted() throws Exception {
        MarkCopyVolumeCompactor compactor =
                new MarkCopyVolumeCompactor(null, null, null, null, null);
        ImageIndex index = new ImageIndex();
        assertFalse(compactor.isDeleted(index));
        index.setFlag(ImageIndex.FLAG_DELETED);
        assertTrue(compactor.isDeleted(index));
    }

    @Test
    public void testMark() throws Exception {
        Configuration config = mock(Configuration.class);
        VolumeContainer container = new PooledVolumeContainer(config);
        Directory directory = mock(Directory.class);

        MarkCopyVolumeCompactor compactor =
                new MarkCopyVolumeCompactor(config, null, container, directory, null);

        long time = System.currentTimeMillis();
        File diskPath = tempFolder.getRoot();
        File volume = new File(diskPath, String.valueOf(time));
        volume.createNewFile();
        Disk disk = new Disk((short) 0, diskPath.getAbsolutePath(), 1.0);

        long mark = compactor.mark(time, disk, volume);
        assertEquals(0L, mark);

        Volume.Writer writer = container.getWriter(volume);
        writer.writeAndFlush(0L, ImageType.JPG, new byte[1024], -1L);
        mark = compactor.mark(time, disk, volume);
        assertEquals(0L, mark);

        writer.writeAndFlush(0L, ImageType.JPG, new byte[1024], 1L);
        mark = compactor.mark(time, disk, volume);
        long expected = 1024L + ImageIndex.FIXED_LENGTH;
        assertEquals(expected, mark);

        writer.writeAndFlush(0L, ImageType.JPG, new byte[2048], time - 1);
        mark = compactor.mark(time, disk, volume);
        expected += (2048 + ImageIndex.FIXED_LENGTH);
        assertEquals(expected, mark);

        writer.writeAndFlush(0L, ImageType.JPG, new byte[2048], time + 1);
        mark = compactor.mark(time, disk, volume);
        assertEquals(expected, mark);
    }

    @Test
    public void testCompact() throws Exception {
        Configuration config = mock(Configuration.class);
        when(config.getVolumeWriteParallelism()).thenReturn(1);
        when(config.getVolumeMaxSize()).thenReturn(1024L * 1024L * 1024L);
        VolumeContainer container = new PooledVolumeContainer(config);
        Store store = new VolumeStore(container);
        Directory directory = mock(Directory.class);

        MarkCopyVolumeCompactor compactor =
                new MarkCopyVolumeCompactor(config, null, container, directory, store);

        long time = System.currentTimeMillis();
        File diskPath = tempFolder.getRoot();
        File volume = new File(diskPath, String.valueOf(time));
        volume.createNewFile();
        Disk disk = new Disk((short) 0, diskPath.getAbsolutePath(), 1.0);

        Volume.Writer writer = container.getWriter(volume);
        writer.writeAndFlush(0L, ImageType.JPG, new byte[1024], 1L);
        writer.writeAndFlush(0L, ImageType.JPG, new byte[1024], -1L);
        writer.writeAndFlush(0L, ImageType.JPG, new byte[1024], time + 1);

        compactor.compact(time, disk, volume);
        List<File> allVolumes = container.getAllVolumes(disk.getPath());
        assertEquals(1, allVolumes.size());
        File newVolume = allVolumes.get(0);
        assertNotEquals(String.valueOf(time), newVolume.getName());

        container.getWriter(newVolume).flush();
        long expected = 2 * (1024 + ImageIndex.FIXED_LENGTH);
        assertEquals(expected, newVolume.length());
    }
}