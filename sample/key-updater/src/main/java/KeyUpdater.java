package com.platform28.archaius.nodeupdater;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.System.exit;

public class KeyUpdater {
    private static final String       RANDOM_STRING_TEMPLATE = "abcdefghijklmnopqrstuvwxyz0123456789";

    public static void main(String[] args) throws Exception {
        String zkHost = args [0];
        String configRootNode = args[1];
        String key = args[2];
        String paths = args[3];
        List<String> pathList = Arrays.asList(paths.split("/"));

        // Startup Curator Framework to talk to Zookeeper
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkHost, new ExponentialBackoffRetry(1000, 3));
        client.start();

        // timer task to randomly add/update/delete key value of random path
        TimerTask task = new TimerTask() {
            public void run() {
                Random rand = new Random();
                int upperbound = pathList.size() + 1;
                int number = rand.nextInt(upperbound);

                String prop = "";
                for (int i = 0; i < number; i++) {
                    if (i < pathList.size()) {
                        prop += (i > 1 ? "/" : "") + pathList.get(i);
                    }
                }
                prop += (!prop.isEmpty() ? "/" : "") + key;

                try {
                    byte[] data = randomString().getBytes("UTF-8");
                    String fullPath = configRootNode + "/" + prop;
                    if (client.checkExists().forPath(fullPath) != null) {
                        int r = rand.nextInt(3);
                        if (r == 2) {
                            System.out.println("Deleted: {key: " + prop + "}");
                            client.delete().deletingChildrenIfNeeded().forPath(fullPath);
                        } else if (r == 1) {
                            System.out.println("Changed: {key: " + prop + ", data: " + new String(data) + "}");
                            client.setData().forPath(fullPath, data);
                        }
                    } else {
                        System.out.println("Added: {key: " + prop + ", data: " + new String(data) + "}");
                        client.create().creatingParentsIfNeeded().forPath(fullPath, data);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Timer timer = new Timer("Timer");

        timer.schedule(task, 0, 5000);

        while (true) {
            System.out.println("Enter \"exit\" to quit.");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            if (s.equals("exit")) {
                client.delete().deletingChildrenIfNeeded().forPath(configRootNode);
                timer.cancel();
                exit(0);
            }
        }
    }

    private static String randomString() {
        StringBuilder builder = new StringBuilder();
        // random 5 letters string
        int count = 5;
        while (count-- != 0) {
            int character = (int) (Math.random() * RANDOM_STRING_TEMPLATE.length());
            builder.append(RANDOM_STRING_TEMPLATE.charAt(character));
        }
        return builder.toString();
    }
}
