package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.topology.Disk;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.context.annotation.Import;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class MapDBImageDirectoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws IOException {
        File path = folder.newFolder();
        Disk disk = new Disk();
        disk.setPath(path.getPath());
        disk.setWeight(1.0);
        MapDBImageDirectory directory = new MapDBImageDirectory();
        Image.Index index = new Image.Index();
        index.setId(10001L);
        index.setOffset(1L);
        directory.save(disk, index);

        Image.Index actual = directory.get(disk, 10001L);
        assertImageIndexEquals(index, actual);

        // update
        index.setOffset(2L);
        directory.save(disk, index);
        actual = directory.get(disk, 10001L);
        assertImageIndexEquals(index, actual);

        directory.delete(disk, 10001L);
        assertNull(directory.get(disk, 10001L));
    }


    private void assertImageIndexEquals(Image.Index expect, Image.Index actual) {
        assertEquals(expect.getId(), actual.getId());
        assertEquals(expect.getOffset(), actual.getOffset());
        assertEquals(expect.getVolume(), actual.getVolume());
        assertEquals(expect.getType(), actual.getType());
        assertEquals(expect.getFlag(), actual.getFlag());
        assertEquals(expect.getSize(), actual.getSize());
        assertEquals(expect.getTime(), actual.getTime());
        assertEquals(expect.getExpireTime(), actual.getExpireTime());
    }
}