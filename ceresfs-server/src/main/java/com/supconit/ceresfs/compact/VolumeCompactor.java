package com.supconit.ceresfs.compact;

public interface VolumeCompactor {

    boolean isRunning();
   
    void compact();
}
