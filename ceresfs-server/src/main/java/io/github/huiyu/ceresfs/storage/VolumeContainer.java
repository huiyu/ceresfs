package io.github.huiyu.ceresfs.storage;

import java.io.File;
import java.util.List;

/**
 * Volume Container
 *
 * @author Yu Hui
 */
public interface VolumeContainer {

    Volume.Writer getActiveWriter(String disk);

    Volume.Writer getWriter(File volume);

    Volume.Reader getReader(File volume);

    List<File> getAllVolumes(String disk);

    void closeVolume(File volume);

    void closeWriter(File volume);

    void closeReader(File volume);

    void deleteVolume(File volume);
}
