package io.github.huiyu.ceresfs.config;

import io.github.huiyu.ceresfs.topology.Disk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ConfigurationServiceTest.TestContextConfig.class)
public class ConfigurationServiceTest {

    @Autowired
    private TestContextConfig contextConfig;
    @Autowired
    private ConfigurationService configurationService;

    @Test
    public void testAutomaticCreateDiskDirectory() {
        assertTrue(contextConfig.folder.exists());
    }

    @Test
    public void testGlobalConfigCodec() throws Exception {
        ConfigurationService.GlobalConfig globalConfig = new ConfigurationService.GlobalConfig();
        globalConfig.setVnodeFactor(1000);
        globalConfig.setReplication((byte) 1);
        byte[] bytes = globalConfig.toBytes();
        globalConfig.fromBytes(bytes);
        assertEquals(1000, globalConfig.getVnodeFactor());
        assertEquals(1, globalConfig.getReplication());
    }

    @Test
    public void testWatchGlobalConfig() throws Exception {
        assertEquals(1, configurationService.getReplication());
        assertEquals(1000, configurationService.getVnodeFactor());

        CuratorFramework client = configurationService.getZookeeperClient();
        assertNotNull(client);

        ConfigurationService.GlobalConfig globalConfig = new ConfigurationService.GlobalConfig();
        globalConfig.setReplication((byte) 2);
        globalConfig.setVnodeFactor(10000);
        byte[] bytes = globalConfig.toBytes();
        client.setData().forPath("/ceresfs/configuration", bytes);

        Thread.sleep(1000L);
        assertEquals(2, configurationService.getReplication());
        assertEquals(10000, configurationService.getVnodeFactor());
        client.close();
    }

    @org.springframework.context.annotation.Configuration
    public static class TestContextConfig implements DisposableBean {

        private TestingServer testingServer;
        private TemporaryFolder tempFolder = new TemporaryFolder();
        private File folder;

        public TestContextConfig() throws Exception {
            testingServer = new TestingServer(true);
            tempFolder.create();
        }

        @Bean
        public ConfigurationService.LocalConfig localConfig() throws IOException {
            ConfigurationService.LocalConfig localConfig = new ConfigurationService.LocalConfig();
            localConfig.setZookeeperAddress(testingServer.getConnectString());
            List<Disk> disks = new ArrayList<>();
            folder = new File(tempFolder.getRoot().getAbsoluteFile(), "testDiskFolder");
            Disk disk = new Disk((short) 0, folder.getAbsolutePath(), 1.0);
            disks.add(disk);
            localConfig.setDisks(disks);
            return localConfig;
        }

        @Bean
        public ConfigurationService.GlobalConfig globalConfig() {
            ConfigurationService.GlobalConfig globalConfig = new ConfigurationService.GlobalConfig();
            globalConfig.setReplication((byte) 1);
            globalConfig.setVnodeFactor(1000);
            return globalConfig;
        }

        @Bean
        public ConfigurationService configurationService() {
            return new ConfigurationService();
        }

        @Override
        public void destroy() throws Exception {
            tempFolder.delete();
            testingServer.close();
        }
    }
}