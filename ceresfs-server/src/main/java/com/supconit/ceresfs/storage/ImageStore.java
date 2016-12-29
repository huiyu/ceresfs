package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.topology.Disk;

import java.io.IOException;

public interface ImageStore {

    Image get(Disk disk, Image.Index index) throws IOException;

    ImageSaveTask prepareSave(Disk disk, long id, Image.Type type, byte[] data) throws IOException;

    void delete(Disk disk, Image.Index index) throws IOException;
}
