package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.topology.Disk;

/**
 * Image directory saves images indexes.
 *
 * @author Yu Hui
 */
public interface ImageDirectory {

    boolean contains(Disk disk, long id);

    /**
     * Get image index
     *
     * @param disk the disk image file located
     * @param id   image id
     * @return image index
     */
    Image.Index get(Disk disk, long id);

    /**
     * Save image index
     *
     * @param disk  the disk image file located
     * @param index image index
     */
    void save(Disk disk, Image.Index index);

    /**
     * Delete image index
     *
     * @param disk the disk image file located
     * @param id   image id
     */
    void delete(Disk disk, long id);
}
