package io.github.huiyu.ceresfs.storage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface Balancer {

    boolean isRunning();

    CompletableFuture<Void> start(long delay, TimeUnit delayTimeUnit);

    void cancel();
}
