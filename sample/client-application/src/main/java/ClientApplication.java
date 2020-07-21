package com.platform28.archaius.application;

import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.DynamicWatchedConfiguration;
import com.platform28.zookeeper.CascadingDynamicPropertyFactory;
import com.platform28.zookeeper.CascadingZooKeeperConfigurationSource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.System.exit;

public class ClientApplication {
    public static void main(String[] args) throws Exception {
        String zkHost = args[0];
        String configRootNode = args[1];
        String key = args[2];
        String fullPath = args[3];

        List<String> pathList = Arrays.asList(fullPath.split("/"));

        // Startup Curator Framework to talk to Zookeeper
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkHost, new ExponentialBackoffRetry(1000, 3));
        client.start();

        // Now configure a CascadingZooKeeperConfigurationSource to allow us to setup keys that cascade on a path.
        CascadingZooKeeperConfigurationSource source = new CascadingZooKeeperConfigurationSource(client, configRootNode);
        try {
            source.start();
        } catch (Exception e) {
            throw new RuntimeException("Cannot start CascadingZooKeeperConfigurationSource.", e);
        }

        // create DynamicWatchedConfiguration
        // this will be added to ConcurrentCompositeConfiguration and installed to ConfigurationManager for DynamicPropertyFactory to cache and update changes from ZooKeeper
        DynamicWatchedConfiguration zkDynamicOverrideConfig = new DynamicWatchedConfiguration(source);
        ConcurrentCompositeConfiguration compositeConfig = new ConcurrentCompositeConfiguration();
        compositeConfig.addConfiguration(zkDynamicOverrideConfig, "zk dynamic override configuration");
        ConfigurationManager.install(compositeConfig);

        CascadingDynamicStringProperty cascadingDynamicStringProperty = new CascadingDynamicStringProperty(fullPath, key, "default");

        // add listener to log the changes if receive any event
        source.addUpdateListener(watchedUpdateResult -> {
            Map<String, Object> added = watchedUpdateResult.getAdded();
            Map<String, Object> updated = watchedUpdateResult.getChanged();
            Map<String, Object> deleted = watchedUpdateResult.getDeleted();

            if (added != null && added.size() > 0) {
                for (Map.Entry<String, Object> entry : added.entrySet()) {
                    System.out.println("Added: " + entry);
                }
            }
            if (updated != null && updated.size() > 0) {
                for (Map.Entry<String, Object> entry : updated.entrySet()) {
                    System.out.println("Updated: " + entry);
                }
            }
            if (deleted != null && deleted.size() > 0) {
                for (Map.Entry<String, Object> entry : deleted.entrySet()) {
                    System.out.println("Deleted: " + entry);
                }
            }
            System.out.println("Current value of CascadingDynamicStringProperty: {key:" + cascadingDynamicStringProperty.getCurrentPropName() + ", value: " + cascadingDynamicStringProperty.get() + "}");
            System.out.println("-----------------------------");
        });
        while (true) {
            System.out.println("Enter \"exit\" to quit.");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            if (s.equals("exit")) {
                exit(0);
            }
        }
    }
}
