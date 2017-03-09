package io.github.huiyu.ceresfs.compact;

public interface VolumeCompactor {

    boolean isRunning();
   
    void compact();
}
