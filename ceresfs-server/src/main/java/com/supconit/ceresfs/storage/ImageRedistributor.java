package com.supconit.ceresfs.storage;

public interface ImageRedistributor {

    boolean isRunning();
    
    void redistribute();

}
