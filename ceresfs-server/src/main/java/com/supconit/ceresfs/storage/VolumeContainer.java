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

    Volume.Writer getAppendableWriter(String disk);

    Volume.Writer getWriter(File volume);

    Volume.Reader getReader(File volume);

    Volume.Updater getUpdater(File volume);

    List<Volume> getAllVolumes(String disk);

    void disableVolume(File volume);

    void disableWriter(File volume);

    void disableReader(File volume);

    void disableUpdater(File volume);

    void deleteVolume(File volume);
}
