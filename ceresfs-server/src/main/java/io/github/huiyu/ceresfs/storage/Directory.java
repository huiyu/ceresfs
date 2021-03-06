package io.github.huiyu.ceresfs.storage;

import io.github.huiyu.ceresfs.topology.Disk;

import java.util.function.Consumer;

/**
 * Image directory saves images indexes.
 *
 * @author Yu Hui
 */
public interface Directory {

    /**
     * Check the image existence
     *
     * @param disk the disk image file located
     * @param id   image id
     * @return weather image exists
     */
    boolean contains(Disk disk, long id);

    /**
     * Get image index
     *
     * @param disk the disk image file located
     * @param id   image id
     * @return image index
     */
    ImageIndex get(Disk disk, long id);

    /**
     * Save image index
     *
     * @param disk  the disk image file located
     * @param index image index
     */
    void save(Disk disk, ImageIndex index);

    /**
     * Delete image index
     *
     * @param disk the disk image file located
     * @param id   image id
     */
    void delete(Disk disk, long id);

    /**
     * Traverse all image ids
     *
     * @param disk     the disk image file located
     * @param consumer customized operation
     */
    void forEachId(Disk disk, Consumer<Long> consumer);

    /**
     * Traverse all image index
     *
     * @param disk     the disk image file located
     * @param consumer customized operation
     */
    void forEachIndex(Disk disk, Consumer<ImageIndex> consumer);

}
