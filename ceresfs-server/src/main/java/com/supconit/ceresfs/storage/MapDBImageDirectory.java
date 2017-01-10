package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.topology.Disk;
import com.supconit.ceresfs.util.Codec;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(prefix = "ceresfs", name = "directory.mode", havingValue = "mapdb", matchIfMissing = true)
public class MapDBImageDirectory implements ImageDirectory, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(MapDBImageDirectory.class);

    private static final String INDEX_FILE = ".metadata";
    private static final String MAP_INDEX = "index";

    private final Map<String, DB> dbByPath = new HashMap<>();
    private final Map<String, HTreeMap<Long, byte[]>> indexByPath = new HashMap<>();

    @Override
    public boolean contains(Disk disk, long id) {
        return getOrCreate(disk).containsKey(id);
    }

    @Override
    public Image.Index get(Disk disk, long id) {
        byte[] data = getOrCreate(disk).get(id);
        if (data == null) {
            return null;
        }
        return ((Image.Index) Codec.decode(data));
    }


    @Override
    public void save(Disk disk, Image.Index index) {
        HTreeMap<Long, byte[]> indexMap = getOrCreate(disk);
        // do save index
        byte[] data = Codec.encode(index);
        indexMap.put(index.getId(), data);
    }

    private HTreeMap<Long, byte[]> getOrCreate(Disk disk) {
        HTreeMap<Long, byte[]> indexMap = indexByPath.get(disk.getPath());
        if (indexMap == null) {
            synchronized (indexByPath) {
                indexMap = indexByPath.get(disk.getPath());
                if (indexMap == null) {
                    indexMap = indexByPath.get(disk.getPath());
                    if (indexMap == null) {
                        createAllStuff(disk.getPath());
                        indexMap = indexByPath.get(disk.getPath());
                    }
                }
            }
        }
        return indexMap;
    }

    private void createAllStuff(String path) {
        LOG.info("Create new index for {}", path);
        DB db = DBMaker
                .fileDB(new File(path, INDEX_FILE))
                .fileMmapEnable()
                .closeOnJvmShutdown()
                .make();
        dbByPath.put(path, db);
        HTreeMap<Long, byte[]> indexMap = db
                .hashMap(MAP_INDEX, Serializer.LONG, Serializer.BYTE_ARRAY)
                .createOrOpen();
        indexByPath.put(path, indexMap);
    }

    @Override
    public void delete(Disk disk, long id) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Delete index {} at {}", id, disk.getPath());
        }
        indexByPath.get(disk.getPath()).remove(id);
    }

    @Override
    public void destroy() throws Exception {
        for (HTreeMap<Long, byte[]> indexMap : indexByPath.values()) {
            indexMap.close();
        }
        for (DB db : dbByPath.values()) {
            db.close();
        }
    }
}
