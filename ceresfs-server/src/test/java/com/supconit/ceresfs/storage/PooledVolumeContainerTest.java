package com.supconit.ceresfs.storage;

import com.google.common.util.concurrent.UncheckedExecutionException;

import com.supconit.ceresfs.config.Configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PooledVolumeContainerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testGetWriter() throws Exception {
        long fileName = System.currentTimeMillis();
        File file = new File(tempFolder.getRoot(), String.valueOf(fileName));

        Configuration config = mockConfig(1024L, 4);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        Volume.Writer writer = container.getWriter(file);
        assertNull(writer);

        file.createNewFile();
        writer = container.getWriter(file);
        assertNotNull(writer);
    }

    @Test
    public void testGetReader() throws Exception {
        long fileName = System.currentTimeMillis();
        File file = new File(tempFolder.getRoot(), String.valueOf(fileName));

        Configuration config = mockConfig(1024L, 4);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        Volume.Reader reader = container.getReader(file);
        assertNull(reader);

        file.createNewFile();
        reader = container.getReader(file);
        assertNotNull(reader);
    }

    @Test
    public void testGetAllVolumes() throws Exception {
        Configuration config = mockConfig(1024L, 4);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        String disk = tempFolder.getRoot().getAbsolutePath();
        List<File> allVolumes = container.getAllVolumes(disk);
        assertNull(allVolumes);

        long currentTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            File file = new File(disk, String.valueOf(currentTimeMillis) + i);
            file.createNewFile();
        }

        allVolumes = container.getAllVolumes(disk);
        assertNotNull(allVolumes);
        assertEquals(100, allVolumes.size());
    }

    @Test
    public void testGetActiveWriter() throws Exception {
        Configuration config = mockConfig(1024L, 1);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        String disk = tempFolder.getRoot().getAbsolutePath();
        Volume.Writer writer = container.getActiveWriter(disk);
        assertNotNull(writer);

        writer.write(1, Image.Type.JPG, new byte[1024], -1L);

        Volume.Writer anotherWriter = container.getActiveWriter(disk);
        assertNotEquals(writer, anotherWriter);
    }

    @Test
    public void testCloseWriter() throws Exception {
        Configuration config = mockConfig(1024L, 1);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        String disk = tempFolder.getRoot().getAbsolutePath();
        Volume.Writer writer = container.getActiveWriter(disk);

        File volume = writer.getVolume();
        container.closeWriter(volume);
        assertTrue(writer.isClosed());

        Volume.Writer anotherWriter = container.getActiveWriter(disk);
        assertNotEquals(writer, anotherWriter);
    }

    @Test
    public void testCloseReader() throws Exception {
        long fileName = System.currentTimeMillis();
        File file = new File(tempFolder.getRoot(), String.valueOf(fileName));
        file.createNewFile();

        Configuration config = mockConfig(1024L, 1);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        Volume.Reader reader = container.getReader(file);
        assertFalse(reader.isClosed());

        container.closeReader(file);
        assertTrue(reader.isClosed());
    }

    @Test
    public void testCloseVolume() throws Exception {
        long fileName = System.currentTimeMillis();
        File file = new File(tempFolder.getRoot(), String.valueOf(fileName));
        file.createNewFile();

        Configuration config = mockConfig(1024L, 1);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        Volume.Writer writer = container.getWriter(file);
        Volume.Reader reader = container.getReader(file);
        assertFalse(writer.isClosed());
        assertFalse(reader.isClosed());

        container.closeVolume(file);
        assertTrue(writer.isClosed());
        assertTrue(reader.isClosed());
        assertTrue(file.exists());
    }

    @Test
    public void testDeleteVolume() throws Exception {

        long fileName = System.currentTimeMillis();
        File file = new File(tempFolder.getRoot(), String.valueOf(fileName));
        file.createNewFile();

        Configuration config = mockConfig(1024L, 1);
        PooledVolumeContainer container = new PooledVolumeContainer(config);

        Volume.Writer writer = container.getWriter(file);
        Volume.Reader reader = container.getReader(file);

        container.deleteVolume(file);
        assertTrue(writer.isClosed());
        assertTrue(reader.isClosed());
        assertFalse(file.exists());
    }

    @Test
    public void testCommonPool() throws Exception {
        PooledVolumeContainer.CommonPool<TestCloseableObject> pool
                = new PooledVolumeContainer.CommonPool<>(5, TimeUnit.SECONDS);
        String key = "key";
        pool.computeIfAbsent(key, () -> new TestCloseableObject(key));
        assertEquals(1, pool.cache.size());

        pool.close();
        assertEquals(0, pool.cache.size());
    }

    @Test
    public void testActiveWriterPool() throws Exception {
        String disk = tempFolder.newFolder().getAbsolutePath();
        PooledVolumeContainer.ActiveWriterPool pool
                = new PooledVolumeContainer.ActiveWriterPool(4, 1024);

        // test get
        Volume.Writer writer = pool.select(disk);
        assertNotNull(writer);
        assertEquals(4, pool.size);
        Volume.Writer[] writers = pool.writersByDisk.get(disk);
        assertNotNull(writers);
        assertEquals(4, writers.length);

        Volume.Writer nextWriter = pool.select(disk);
        assertNotEquals(writer, nextWriter);

        // test disable
        pool.disable(writer.getVolume());
        assertEquals(4, writers.length);
        assertTrue(writer.isClosed());

        // test close
        pool.close();
        assertTrue(nextWriter.isClosed());
    }

    private Configuration mockConfig(long volumeMaxSize, int volumeWriteParallelism) {
        Configuration config = mock(Configuration.class);
        when(config.getVolumeMaxSize()).thenReturn(volumeMaxSize);
        when(config.getVolumeWriteParallelism()).thenReturn(volumeWriteParallelism);
        return config;
    }

    private static final class TestCloseableObject implements Closeable {

        boolean close = false;
        final String key;

        TestCloseableObject(String key) {
            this.key = key;
        }

        @Override
        public void close() throws IOException {
            this.close = true;
        }
    }

}