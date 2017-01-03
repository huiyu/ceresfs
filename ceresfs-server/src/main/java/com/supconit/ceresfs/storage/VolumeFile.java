package com.supconit.ceresfs.storage;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class VolumeFile {

    private final File file;

    public VolumeFile(File file) {
        this.file = file;
    }

    public VolumeFile(String path) {
        this.file = new File(path);
    }

    public File getFile() {
        return file;
    }

    public static Reader createReader(File file) throws FileNotFoundException {
        return new Reader(file);
    }

    public static Writer createWriter(File file) throws FileNotFoundException {
        return new Writer(file);
    }

    public static class Reader implements Closeable {

        private RandomAccessFile file;

        public Reader(File file) throws FileNotFoundException {
            this.file = new RandomAccessFile(file, "r");
        }

        public synchronized void seek(long pos) throws IOException {
            file.seek(pos);
        }

        public synchronized Image read(long pos) throws IOException {
            seek(pos);
            return next();
        }

        public synchronized Image next() throws IOException {
            // read index
            
            byte[] head = new byte[Image.Index.FIXED_LENGTH];
            int bytesRead = file.read(head);
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
            file.read(data);
            Image image = new Image(index, data);
            return image;
        }

        @Override
        public void close() throws IOException {
            this.file.close();
        }
    }

    public static class Writer implements Closeable {

        private static final byte[] PADDING = {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        private BufferedOutputStream out;
        private long size;
        private File file;

        public Writer(File file) throws FileNotFoundException {
            this.file = file;
            this.size = file.length();
            this.out = new BufferedOutputStream(new FileOutputStream(file, true));
        }

        public synchronized void write(Image image) throws IOException {
            Image.Index index = image.getIndex(); // 46 bytes, padding to 64 bytes
            byte[] data = image.getData();

            // offset and total size
            index.setOffset(size);
            index.setSize(data.length);
            index.setVolume(Long.valueOf(file.getName()));

            // set time
            index.setTime(System.currentTimeMillis());

            // set volume file name
            index.setVolume(Long.valueOf(file.getName()));

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
        }

        public synchronized void flush() throws IOException {
            this.out.flush();
        }

        @Override
        public void close() throws IOException {
            this.out.close();
        }
    }
}
