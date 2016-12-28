package com.supconit.ceresfs.storage;

import com.supconit.ceresfs.topology.Disk;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.*;

public class ImageStoreWorkerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    private Disk disk;
    
    @Before
    public void setup() throws IOException {
        folder.create();
    }
    
    @Test
    public void test() throws Exception {
        
    }
}