package com.platform28.zookeeper;

import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.ConcurrentMapConfiguration;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicWatchedConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.DebugUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class CascadingZooKeeperConfigurationSourceTest {
    private static final Logger logger = LoggerFactory.getLogger(CascadingZooKeeperConfigurationSourceTest.class);

    private static final String                                CONFIG_ROOT_PATH = "/config";
    private static       TestingServer                         server;
    private static       CuratorFramework                      client;
    private static       CascadingZooKeeperConfigurationSource cascadingZooKeeperConfigurationSource;
    private static       ConcurrentMapConfiguration            mapConfig;

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
        server = new TestingServer();
        logger.info("Initialized local ZK with connect string [{}]", server.getConnectString());

        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        client.start();

        cascadingZooKeeperConfigurationSource = new CascadingZooKeeperConfigurationSource(client, CONFIG_ROOT_PATH);
        cascadingZooKeeperConfigurationSource.start();

        final ConcurrentMapConfiguration systemConfig = new ConcurrentMapConfiguration();
        systemConfig.loadProperties(System.getProperties());

        final DynamicWatchedConfiguration zkDynamicOverrideConfig = new DynamicWatchedConfiguration(cascadingZooKeeperConfigurationSource);

        mapConfig = new ConcurrentMapConfiguration();
        mapConfig.addProperty("test.key3", "test.value3-map");

        final ConcurrentCompositeConfiguration compositeConfig = new ConcurrentCompositeConfiguration();
        compositeConfig.addConfiguration(zkDynamicOverrideConfig, "zk dynamic override configuration");
        compositeConfig.addConfiguration(mapConfig, "map configuration");

        // setup ZK properties
        // path is null -> set property to root node
        setZkProperty(null, "test.key1", "test.value1-zk");
        setZkProperty(null, "test.key2", "test.value2-zk");
        setZkProperty("/child_1", "test.key1", "child_1.test-key1");
        setZkProperty("/child_1", "key1", "child_1.key1");
        setZkProperty("/child_1/child_2", "test.key1", "child_1.child_2.test-key1-override");

        ConfigurationManager.install(compositeConfig);
    }

    @Test
    public void testNoPropertyFound() throws Exception {
        Assert.assertEquals("default", CascadingDynamicPropertyFactory
                .getStringProperty(null, "key10000", "default").get());

        Assert.assertEquals("default", CascadingDynamicPropertyFactory
                .getStringProperty("/not_existing", "key10000", "default").get());
    }

    @Test
    public void testNoPropertyOverride() throws Exception {
        Assert.assertEquals("test.value3-map", CascadingDynamicPropertyFactory
                .getStringProperty(null, "test.key3", "default").get());
    }

    @Test
    public void testCascadingDynamicPropertyFactory() throws Exception {
        // cascading: should return value of property child_1/key1 = child_1.key1
        Assert.assertEquals("child_1.key1", CascadingDynamicPropertyFactory
                .getStringProperty("/child_1/child_2", "key1", "default").get());

        Assert.assertEquals("test.value2-zk", CascadingDynamicPropertyFactory
                .getStringProperty("/child_1/child_2", "test.key2", "default").get());
    }

    @Test
    public void testPropertyOverride() throws Exception {
        setZkProperty(null, "test.key4", "test-key4-zk-override");
        mapConfig.setProperty("test.key4", "prod");

        // should return value test-key3-zk-override
        Assert.assertEquals("test-key4-zk-override", CascadingDynamicPropertyFactory
                .getStringProperty(null, "test.key4", "default").get());
    }

    @Test
    public void testUpdateProperty() throws Exception {
        setZkProperty(null, "test.key1", "test.value1-zk-override");
        setZkProperty("/child_1", "test.key1", "child_1.test-key1-override");
        setZkProperty("/child_1/child_2", "test.key1", "child_1.child_2.key1-override");

        Assert.assertEquals("test.value1-zk-override", CascadingDynamicPropertyFactory
                .getStringProperty(null, "test.key1", "default").get());

        Assert.assertEquals("child_1.test-key1-override", CascadingDynamicPropertyFactory
                .getStringProperty("/child_1", "test.key1", "default").get());

        Assert.assertEquals("child_1.child_2.key1-override", CascadingDynamicPropertyFactory
                .getStringProperty("/child_1/child_2", "test.key1", "default").get());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cascadingZooKeeperConfigurationSource.close();
        server.close();
        logger.info("Tore down embedded ZK with connect string [{}]", server.getConnectString());
    }

    private static void setZkProperty(String path, String key, String value) throws Exception {
        // update the underlying zk property and assert that the new value is picked up
        final CountDownLatch updateLatch = new CountDownLatch(1);
        cascadingZooKeeperConfigurationSource.addUpdateListener(result -> updateLatch.countDown());
        cascadingZooKeeperConfigurationSource.setZkProperty(path, key, value);
        updateLatch.await();
    }
}
