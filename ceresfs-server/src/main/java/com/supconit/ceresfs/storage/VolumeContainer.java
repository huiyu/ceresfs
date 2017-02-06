package com.supconit.ceresfs.storage;

import java.io.File;
import java.util.List;

/**
 * Volume Container
 *
 * @author Yu Hui
 */
public interface VolumeContainer {

    Volume getVolume(File volume);

    Volume.Writer getActiveWriter(String disk);

    Volume.Writer getWriter(File volume);

    Volume.Reader getReader(File volume);

    Volume.Updater getUpdater(File volume);

    List<Volume> getAllVolumes(String disk);

    void closeVolume(File volume);

    void closeWriter(File volume);

    void closeReader(File volume);

    void closeUpdater(File volume);

    void deleteVolume(File volume);
}
