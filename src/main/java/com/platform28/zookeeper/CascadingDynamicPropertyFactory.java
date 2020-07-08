package com.platform28.zookeeper;

import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

import java.util.StringTokenizer;

public class CascadingDynamicPropertyFactory {

    public static DynamicStringProperty getStringProperty(String path, String key, String defaultValue) {

        DynamicPropertyFactory dynamicPropertyFactory = DynamicPropertyFactory.getInstance();
        if (path == null || path.isEmpty()) {
            return dynamicPropertyFactory.getStringProperty(key, defaultValue, null);
        }
        String currentPath = path;
        // remove the first "/", ie: /child_1/child_2 -> child_1/child_2
        // since the DynamicPropertyFactory will append the "/rootnode/" to it
        if (currentPath.startsWith("/")) {
            currentPath = currentPath.substring(1);
        }

        StringTokenizer pathToken = new StringTokenizer(currentPath, "/");
        return getStringProperty(pathToken, null, key, defaultValue);
    }

    private static DynamicStringProperty getStringProperty(StringTokenizer pathTokens, String curPath, String key, String defaultValue) {
        DynamicPropertyFactory dynamicPropertyFactory = DynamicPropertyFactory.getInstance();
        DynamicStringProperty dynamicStringProperty = null;
        curPath = curPath != null && !curPath.isEmpty() ? curPath + "/" : "";
        while(pathTokens.hasMoreTokens()) {
            dynamicStringProperty = getStringProperty(pathTokens, curPath + pathTokens.nextToken(), key, defaultValue);
        }
        if (dynamicStringProperty == null || !dynamicStringProperty.getDynamicProperty().getCachedValue(String.class).isPresent()) {
            dynamicStringProperty = dynamicPropertyFactory.getStringProperty(curPath + key, defaultValue, null);
        }
        return dynamicStringProperty;
    }
}
