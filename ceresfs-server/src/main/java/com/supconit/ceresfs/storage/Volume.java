package com.supconit.ceresfs.storage;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO performance improvement
 */
public class Volume {

    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final File file;

    public Volume(File file) {
        this.file = file;
    }

    public static Reader createReader(Volume volume) throws FileNotFoundException {
        return new Reader(volume);
    }

    public static Writer createWriter(Volume volume) throws FileNotFoundException {
        return new Writer(volume);
    }

    public static Updater createUpdater(Volume volume) throws FileNotFoundException {
        return new Updater(volume);
    }

    public ReentrantLock getWriteLock() {
        return writeLock;
    }

    public File getFile() {
        return file;
    }

    public ReentrantLock getReadLock() {
        return readLock;
    }

    public static final class Reader implements Closeable {

        private final RandomAccessFile raf;
        private final Volume volume;
        private volatile boolean closed = false;

        private Reader(Volume volume) throws FileNotFoundException {
            this.volume = volume;
            this.raf = new RandomAccessFile(volume.getFile(), "r");
        }

        public void seek(long pos) throws IOException {
            final ReentrantLock lock = volume.getReadLock();
            lock.lock();
            try {
                raf.seek(pos);
            } finally {
                lock.unlock();
            }
        }

        public Image read(long pos) throws IOException {
            final ReentrantLock lock = volume.getReadLock();
            lock.lock();
            try {
                seek(pos);
                return next();
            } finally {
                lock.unlock();
            }
        }

        public Image next() throws IOException {
            final ReentrantLock lock = volume.getReadLock();
            lock.lock();
            try {
                // read index
                byte[] head = new byte[Image.Index.FIXED_LENGTH];
                int bytesRead = raf.read(head);
                if (bytesRead < Image.Index.FIXED_LENGTH) {
                    return null;
                }
                ByteBuffer buffer = ByteBuffer.wrap(head);
                Image.Index index = new Image.Index();
                index.setId(buffer.getLong());
                index.setVolume(buffer.getLong());
                index.setFlag(buffer.get());
                index.setOffset(buffer.getLong());
                index.setSize(buffer.getInt());
                index.setType(Image.Type.parse(buffer.get()));
                index.setTime(buffer.getLong());
                index.setExpireTime(buffer.getLong());
                index.setReplication(buffer.get());
                // read image data
                byte[] data = new byte[index.getSize()];
                raf.read(data);
                Image image = new Image(index, data);
                return image;
            } finally {
                lock.unlock();
            }
        }

        public Volume getVolume() {
            return volume;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            final ReentrantLock lock = volume.getReadLock();
            lock.lock();
            try {
                this.closed = true;
                this.raf.close();
            } finally {
                lock.unlock();
            }
        }
    }

    public static final class Writer implements Closeable {

        private static final byte[] PADDING = {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        private final BufferedOutputStream out;
        private final Volume volume;
        private final long volumeId;
        private volatile long size;
        private volatile boolean closed = false;

        private Writer(Volume volume) throws FileNotFoundException {
            this.volume = volume;
            this.volumeId = Long.valueOf(volume.getFile().getName());
            this.out = new BufferedOutputStream(new FileOutputStream(volume.getFile(), true));
        }

        public void write(Image image) throws IOException {
            final ReentrantLock lock = volume.getWriteLock();
            lock.lock();
            try {
                Image.Index index = image.getIndex(); // 46 bytes, padding to 64 bytes
                byte[] data = image.getData();

                // offset and total size
                index.setOffset(size);
                index.setSize(data.length);
                index.setVolume(volumeId);
                // set time
                index.setTime(System.currentTimeMillis());
                // set volume file name
                index.setVolume(volumeId);

                int capacity = 64 + image.getData().length;
                ByteBuffer buffer = ByteBuffer.allocate(capacity);
                // put index
                buffer.putLong(index.getId());
                buffer.putLong(index.getVolume());
                buffer.put(index.getFlag());
                buffer.putLong(index.getOffset());
                buffer.putInt(index.getSize());
                buffer.put(index.getType().getCode());
                buffer.putLong(index.getTime());
                buffer.putLong(index.getExpireTime());
                buffer.put(index.getReplication());
                buffer.put(PADDING);
                // put data
                buffer.put(data);
                out.write(buffer.array());
                size += capacity;
            } finally {
                lock.unlock();
            }
        }

        public void write(long id, Image.Type type, byte[] data, long expireTime) throws IOException {
            Image.Index index = new Image.Index();
            index.setId(id);
            index.setType(type);
            index.setExpireTime(expireTime);
            Image image = new Image(index, data);
            write(image);
        }

        public void flush() throws IOException {
            final ReentrantLock lock = volume.getWriteLock();
            lock.lock();
            try {
                out.flush();
            } finally {
                lock.unlock();
            }
        }

        public void writeAndFlush(Image image) throws IOException {
            final ReentrantLock lock = volume.getWriteLock();
            lock.lock();
            try {
                write(image);
                out.flush();
            } finally {
                lock.unlock();
            }
        }

        public Volume getVolume() {
            return volume;
        }

        public long length() {
            return size;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            final ReentrantLock lock = volume.getWriteLock();
            lock.lock();
            try {
                this.closed = true;
                this.out.close();
            } finally {
                lock.unlock();
            }
        }
    }

    public static final class Updater implements Closeable {

        private final RandomAccessFile raf;
        private final Volume volume;
        private volatile boolean closed = false;

        private Updater(Volume volume) throws FileNotFoundException {
            this.volume = volume;
            this.raf = new RandomAccessFile(volume.getFile(), "rw");
        }

        public void markDeleted(long pos) throws IOException {
            mark(pos, Image.FLAG_DELETED);
        }

        public void markNormal(long pos) throws IOException {
            mark(pos, Image.FLAG_NORMAL);
        }

        private void mark(long pos, byte flag) throws IOException {
            final ReentrantLock lock = volume.getWriteLock();
            lock.lock();
            try {
                raf.seek(pos + 16);
                raf.writeByte(flag);
            } finally {
                lock.unlock();
            }
        }

        public Volume getVolume() {
            return volume;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            final ReentrantLock lock = volume.getWriteLock();
            lock.lock();
            try {
                this.closed = true;
                this.raf.close();
            } finally {
                lock.unlock();
            }
        }
    }
}
