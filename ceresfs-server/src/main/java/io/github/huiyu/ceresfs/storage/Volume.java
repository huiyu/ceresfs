package io.github.huiyu.ceresfs.storage;

import io.github.huiyu.ceresfs.ImageType;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class Volume {

    private Volume() {
    }

    public static Reader createReader(File volume) throws FileNotFoundException {
        return new Reader(volume);
    }

    public static Writer createWriter(File volume) throws FileNotFoundException {
        return new Writer(volume);
    }

    public static final class Reader implements Closeable {

        private final ReentrantLock lock = new ReentrantLock();
        private final RandomAccessFile raf;
        private final File volume;
        private volatile boolean closed = false;

        private Reader(File volume) throws FileNotFoundException {
            this.volume = volume;
            this.raf = new RandomAccessFile(volume, "r");
        }

        public void seek(long pos) throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                raf.seek(pos);
            } finally {
                lock.unlock();
            }
        }

        public Image read(long pos) throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                seek(pos);
                return next();
            } finally {
                lock.unlock();
            }
        }

        public Image next() throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                // read index
                byte[] head = new byte[ImageIndex.FIXED_LENGTH];
                int bytesRead = raf.read(head);
                if (bytesRead < ImageIndex.FIXED_LENGTH) {
                    return null;
                }
                ByteBuffer buffer = ByteBuffer.wrap(head);
                ImageIndex index = new ImageIndex();
                index.setId(buffer.getLong());
                index.setVolume(buffer.getLong());
                index.setFlag(buffer.get());
                index.setOffset(buffer.getLong());
                index.setSize(buffer.getInt());
                index.setType(ImageType.fromCode(buffer.get()));
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

        public File getVolume() {
            return volume;
        }

        public ReentrantLock getLock() {
            return lock;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            final ReentrantLock lock = this.lock;
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

        private final ReentrantLock lock = new ReentrantLock();
        private final RandomAccessFile raf;
        private final BufferedOutputStream out;
        private final File volume;
        private final long volumeId;
        private volatile long size;
        private volatile boolean closed = false;

        private Writer(File volume) throws FileNotFoundException {
            this.volume = volume;
            this.volumeId = Long.valueOf(volume.getName());
            // FIXME: combine out and raf into a single BufferedRandomAccessFile
            this.out = new BufferedOutputStream(new FileOutputStream(volume, true));
            this.raf = new RandomAccessFile(volume, "rw");
        }

        public void write(Image image) throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                ImageIndex index = image.getIndex(); // 46 bytes, padding to 64 bytes
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

        public void write(long id, ImageType type, byte[] data, long expireTime) throws IOException {
            ImageIndex index = new ImageIndex();
            index.setId(id);
            index.setType(type);
            index.setExpireTime(expireTime);
            Image image = new Image(index, data);
            write(image);
        }

        public void flush() throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                out.flush();
            } finally {
                lock.unlock();
            }
        }

        public void writeAndFlush(Image image) throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                write(image);
                out.flush();
            } finally {
                lock.unlock();
            }
        }

        public void writeAndFlush(long id, ImageType type, byte[] data, long expireTime)
                throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                write(id, type, data, expireTime);
                out.flush();
            } finally {
                lock.unlock();
            }
        }

        public void markDeleted(long pos) throws IOException {
            mark(pos, ImageIndex.FLAG_DELETED);
        }

        public void markNormal(long pos) throws IOException {
            mark(pos, ImageIndex.FLAG_NORMAL);
        }

        private void mark(long pos, byte flag) throws IOException {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                raf.seek(pos + 16);
                raf.writeByte(flag);
            } finally {
                lock.unlock();
            }
        }

        public ReentrantLock getLock() {
            return lock;
        }

        public File getVolume() {
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
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                this.closed = true;
                this.out.close();
            } finally {
                lock.unlock();
            }
        }
    }
}
