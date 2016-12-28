package com.supconit.ceresfs.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

import com.supconit.ceresfs.EventHandler;
import com.supconit.ceresfs.topology.Disk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Component
public class VolumeImageStore implements ImageStore, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(VolumeImageStore.class);

    private final Map<String, ImageStoreWorker> workerByPath = new HashMap<>();
    private final Cache<String, VolumeFile.Reader> readerByPath = CacheBuilder
            .newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .concurrencyLevel(4)
            .removalListener((RemovalListener<String, VolumeFile.Reader>) notification -> {
                try {
                    LOG.debug("Close volume[{}] reader", notification.getKey());
                    notification.getValue().close();
                } catch (Exception e) {
                    LOG.error("Close reader [" + notification.getKey() + "] error.", e);
                }
            })
            .build();

    @Value("${ceresfs.volume.limit:107374182400}")
    private long volumeLimit;

    @Override
    public Image get(Disk disk, Image.Index index) throws IOException {
        ImageStoreWorker worker = getOrCreateWorker(disk);
        Image image = worker.find(index.getId());
        if (image != null) {
            return image;
        }
        File file = new File(disk.getPath(), String.valueOf(index.getVolume()));
        VolumeFile.Reader reader = getOrCreateReader(file);
        reader.seek(index.getOffset());
        image = reader.readImage();
        return image;
    }

    @Override
    public ImageSaveTask save(Disk disk, long id, Image.Type type, byte[] data) {
        return new DefaultImageSaveTask(this, disk, id, type, data);
    }

    @Override
    public void delete(Disk disk, Image.Index index) throws IOException {
        File file = new File(disk.getPath(), String.valueOf(index.getVolume()));
        VolumeFile.Reader reader = getOrCreateReader(file);
        // TODO
    }

    private VolumeFile.Reader getOrCreateReader(File file) throws IOException {
        try {
            return readerByPath.get(file.getPath(), () -> VolumeFile.createReader(file));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw ((IOException) e.getCause());
            }
            throw new IOException(e);
        }
    }

    private ImageStoreWorker getOrCreateWorker(Disk disk) throws IOException {
        ImageStoreWorker worker = workerByPath.get(disk.getPath());
        if (worker == null) {
            synchronized (workerByPath) {
                LOG.info("Start new worker {}", disk.getPath());
                worker = workerByPath.get(disk.getPath());
                if (worker == null) {
                    worker = new ImageStoreWorker(disk.getPath(), volumeLimit);
                    worker.start();
                    workerByPath.put(disk.getPath(), worker);
                }
            }
        }
        return worker;
    }

    @Override
    public void destroy() throws Exception {
        readerByPath.cleanUp();
        workerByPath.values().forEach(ImageStoreWorker::shutdown);
    }

    private static class DefaultImageSaveTask implements ImageSaveTask {

        private Image image;
        private EventHandler<Image> successHandler =
                e -> LOG.debug("Image[id={}] save success", e.getIndex().getId());
        private EventHandler<ImageSaveError> errorHandler = e -> {
            String msg = "Image[id={" + e.getImage().getIndex().getId() + "}] save error";
            LOG.error(msg, e.getError());
        };
        private VolumeImageStore store;
        private Disk disk;


        public DefaultImageSaveTask(VolumeImageStore store, Disk disk, long id, Image.Type type, byte[] data) {
            this.store = store;
            this.disk = disk;
            Image.Index index = new Image.Index();
            index.setId(id);
            index.setType(type);
            index.setFlag(Image.FLAG_NORMAL);
            index.setTime(System.currentTimeMillis());
            index.setExpireTime(-1L);
            this.image = new Image(index, data);
        }

        @Override
        public ImageSaveTask setTime(long time) {
            this.image.getIndex().setTime(time);
            return this;
        }

        @Override
        public ImageSaveTask setExpireTime(long expireTime) {
            Assert.isTrue(expireTime > image.getIndex().getTime() || expireTime < 0,
                    "Expire time must > upload time or < 0");
            this.image.getIndex().setExpireTime(expireTime);
            return this;
        }

        @Override
        public ImageSaveTask onSuccess(EventHandler<Image> handler) {
            this.successHandler = handler;
            return this;
        }

        @Override
        public ImageSaveTask onError(EventHandler<ImageSaveError> handler) {
            this.errorHandler = handler;
            return this;
        }

        @Override
        public void execute(boolean sync) {
            if (sync) {
                // TODO wait to complete
            } else {
                try {
                    ImageStoreWorker worker = store.getOrCreateWorker(disk);
                    worker.put(this);
                } catch (IOException e) {
                    this.errorHandler.handle(new ImageSaveError(image, e));
                }
            }
        }
    }

    private static class ImageStoreWorker extends Thread {

        private final long sizeLimit;
        private final String diskPath;
        private final LinkedBlockingDeque<DefaultImageSaveTask> queue = new LinkedBlockingDeque<>();

        private volatile File volumeFile;
        private volatile VolumeFile.Writer volumeFileWriter;
        private volatile long fileOffset;

        private boolean running;

        public ImageStoreWorker(String diskPath, long sizeLimit) throws IOException {
            this.diskPath = diskPath;
            this.sizeLimit = sizeLimit;
            File path = new File(diskPath);
            File[] files = path.listFiles(pathname -> pathname.getName().matches("\\d*"));
            if (files == null || files.length == 0) {
                newVolumeFile();
            } else {
                File latest = getLatestVolume(files);
                if (latest.length() >= sizeLimit) {
                    newVolumeFile();
                } else {
                    volumeFile = latest;
                    fileOffset = latest.length();
                }
            }

            this.volumeFileWriter = VolumeFile.createWriter(volumeFile);
            this.running = true;
        }

        public void put(DefaultImageSaveTask task) throws IOException {
            int length = task.image.getData().length;
            if (length + fileOffset > sizeLimit) {
                newVolumeFile();
            }

            queue.offer(task);
            fileOffset += length;
        }


        public Image find(long id) {
            for (DefaultImageSaveTask task : queue) {
                Image image = task.image;
                if (image.getIndex().getId() == id) {
                    return image;
                }
            }
            return null;
        }

        @Override
        public void run() {
            while (running) {
                DefaultImageSaveTask task = take();
                Image image = task.image;
                try {
                    this.volumeFileWriter.write(image);
                    this.volumeFileWriter.flush();
                    task.successHandler.handle(image);
                } catch (Exception e) {
                    task.errorHandler.handle(new ImageSaveError(image, e));
                }
            }
        }

        private DefaultImageSaveTask take() {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                // FIXME may cause npe?
                LOG.error("Image queue tak error, ", e);
                return null;
            }
        }

        private void newVolumeFile() throws IOException {
            // using timestamp as volume file name
            String fileName = String.valueOf(System.currentTimeMillis());
            File file = new File(diskPath, fileName);
            boolean created = file.createNewFile();
            if (!created) {
                throw new IOException("Can't create new volume file.");
            }
            this.volumeFile = file;
            this.fileOffset = 0L;

        }

        private File getLatestVolume(File[] files) {
            int length = files.length;
            if (length == 0) {
                return null;
            }
            if (length == 1) {
                return files[0];
            }

            File latest = files[0];
            long latestTimestamp = Long.valueOf(latest.getName());
            for (int i = 1; i < length; i++) {
                File file = files[i];
                long timestamp = Long.valueOf(file.getName());
                if (timestamp > latestTimestamp) {
                    latest = file;
                    latestTimestamp = timestamp;
                }

            }
            return latest;
        }

        public synchronized void shutdown() {
            while (!queue.isEmpty()) {
                // FIXME
                await();
            }
            this.running = false;
        }

        private void await() {
            try {
                wait(100);
            } catch (InterruptedException e) {
            }
        }
    }
}
