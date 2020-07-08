package com.platform28.zookeeper;

import com.google.common.io.Closeables;
import com.netflix.config.WatchedConfigurationSource;
import com.netflix.config.WatchedUpdateListener;
import com.netflix.config.WatchedUpdateResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class CascadingZooKeeperConfigurationSource implements WatchedConfigurationSource, Closeable {

    private static final Log logger = LogFactory.getLog(CascadingZooKeeperConfigurationSource.class);

    private final CuratorFramework client;
    private final String           configRootPath;
    private final TreeCache        treeCache;

    private final Charset charset = Charset.forName("UTF-8");

    private List<WatchedUpdateListener> listeners = new CopyOnWriteArrayList<WatchedUpdateListener>();

    public CascadingZooKeeperConfigurationSource(CuratorFramework client, String configRootPath) {
        this.client = client;
        this.configRootPath = configRootPath;
        this.treeCache = new TreeCache(client, configRootPath);
    }

    public void start() throws Exception {
        // create the watcher for future configuration updatess
        treeCache.getListenable().addListener((aClient, event) -> {
            TreeCacheEvent.Type eventType = event.getType();
            ChildData data = event.getData();

            String path = null;
            if (data != null) {
                path = data.getPath();

                // scrub configRootPath out of the key name
                String key = removeRootPath(path);

                byte[] value = data.getData();
                String stringValue = new String(value, charset);

                logger.debug("Received update to pathName " + path + ", eventType " + eventType
                        + " with key " + key + ", and value " + stringValue);

                // fire event to all listeners
                Map<String, Object> added = null;
                Map<String, Object> changed = null;
                Map<String, Object> deleted = null;
                if (eventType == TreeCacheEvent.Type.NODE_ADDED) {
                    added = new HashMap<>(1);
                    added.put(key, stringValue);
                } else if (eventType == TreeCacheEvent.Type.NODE_UPDATED) {
                    changed = new HashMap<>(1);
                    changed.put(key, stringValue);
                } else if (eventType == TreeCacheEvent.Type.NODE_REMOVED) {
                    deleted = new HashMap<>(1);
                    deleted.put(key, stringValue);
                }

                WatchedUpdateResult result = WatchedUpdateResult.createIncremental(added,
                        changed, deleted);

                fireEvent(result);
            }
        });

        treeCache.start();
    }

    @Override
    public Map<String, Object> getCurrentData() throws Exception {
        logger.debug("getCurrentData() retrieving current data.");
        return getCurrentData(configRootPath);
    }

    private Map<String, Object> getCurrentData(String path) {
        Map<String, ChildData> childDataMap = treeCache.getCurrentChildren(path);
        Map<String, Object> result = new HashMap<String, Object>();
        if (childDataMap != null) {
            for (ChildData childData : childDataMap.values()) {
                if (childData.getData().length > 0) {
                    result.put(removeRootPath(childData.getPath()), new String(childData.getData(), charset));
                } else {
                    result.putAll(getCurrentData(childData.getPath()));
                }
            }
        }
        return result;
    }

    @Override
    public void addUpdateListener(WatchedUpdateListener l) {
        if (l != null) {
            listeners.add(l);
        }
    }

    @Override
    public void removeUpdateListener(WatchedUpdateListener l) {
        if (l != null) {
            listeners.remove(l);
        }
    }

    protected void fireEvent(WatchedUpdateResult result) {
        for (WatchedUpdateListener l : listeners) {
            try {
                l.updateConfiguration(result);
            } catch (Throwable ex) {
                logger.error("Error in invoking WatchedUpdateListener", ex);
            }
        }
    }

    private String removeRootPath(String nodePath) {
        return nodePath.replace(configRootPath + "/", "");
    }

    // this function is for Testing only
    synchronized void setZkProperty(String path, String key, String value) throws Exception {
        final String fullPath = configRootPath + (path != null && !path.isEmpty() ? path : "") + "/" + key;

        byte[] data = value.getBytes(charset);

        try {
            // attempt to create (intentionally doing this instead of checkExists())
            client.create().creatingParentsIfNeeded().forPath(fullPath, data);
        } catch (KeeperException.NodeExistsException exc) {
            // key already exists - update the data instead
            client.setData().forPath(fullPath, data);
        }
    }

    // this function is for Testing only
    synchronized String getZkProperty(String path, String key) throws Exception {
        final String fullPath = configRootPath + (path != null && !path.isEmpty() ? path : "") + "/" + key;

        byte[] bytes = client.getData().forPath(fullPath);

        return new String(bytes, charset);
    }

    // this function is for Testing only
    synchronized void deleteZkProperty(String path, String key) throws Exception {
        final String fullPath = configRootPath + (path != null && !path.isEmpty() ? path : "") + "/" + key;

        try {
            client.delete().forPath(fullPath);
        } catch (KeeperException.NoNodeException exc) {
            // Node doesn't exist - NoOp
            logger.warn("Node doesn't exist", exc);
        }
    }

    public void close() {
        try {
            Closeables.close(treeCache, true);
        } catch (IOException exc) {
            logger.error("IOException should not have been thrown.", exc);
        }
    }
}
