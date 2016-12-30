package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.topology.Disk;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

        Image.Index get = directory.get(disk, 10001L);
        assertEquals(get, index);

        index.setOffset(2L);
        directory.save(disk, index);
        get = directory.get(disk, 10001L);
        assertEquals(get, index);

        directory.delete(disk, 10001L);
        assertNull(directory.get(disk, 10001L));
    }
}