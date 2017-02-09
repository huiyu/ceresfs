package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.retry.RetryStrategy;
import com.supconit.ceresfs.topology.Disk;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Image Store
 *
 * @author Yu Hui
 */
public interface Store {

    /**
     * Get image by index
     *
     * @param disk  the disk image located
     * @param index image index
     * @return the image
     */
    Image get(Disk disk, Image.Index index) throws IOException;

    /**
     * Save image permanently using default retry strategy
     *
     * @param disk the disk image located
     * @param id   image id
     * @param type image type
     * @param data image data
     * @return an instance of <code>CompletableFuture</code>, which allow you to process result
     * synchronously or asynchronously
     */
    CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data);


    /**
     * Save image using default retry strategy
     *
     * @param disk       the disk image located
     * @param id         image id
     * @param type       image type
     * @param data       image data
     * @param expireTime the image expired timestamp
     * @return an instance of <code>CompletableFuture</code>, which allow you to process result
     * either synchronously or asynchronously
     */
    CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data, long expireTime);

    /**
     * Save image data permanently using specified retry strategy
     *
     * @param disk          the disk image located
     * @param id            image id
     * @param type          image type
     * @param data          image data
     * @param retryStrategy retry strategy
     * @return an instance of <code>CompletableFuture</code>, which allow you to process result
     * either synchronously or asynchronously
     */
    CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data,
                                  RetryStrategy retryStrategy);

    /**
     * Save image data using specified expire time and retry strategy
     *
     * @param disk          the disk image located
     * @param id            image id
     * @param type          image type
     * @param data          image data
     * @param expireTime    the image expire timestamp
     * @param retryStrategy retry strategy
     * @return an instance of <code>CompletableFuture</code>, which allow you to process result
     * either synchronously or asynchronously
     */
    CompletableFuture<Image> save(Disk disk, long id, Image.Type type, byte[] data, long expireTime,
                                  RetryStrategy retryStrategy);

    /**
     * Delete image data
     *
     * @param disk  the disk image located
     * @param index image index
     */
    void delete(Disk disk, Image.Index index) throws IOException;
}
