package com.platform28.zookeeper;

import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

import java.util.HashMap;
import java.util.Map;

public class CascadingDynamicStringProperty {
    private Map<Integer, DynamicStringProperty> dynamicStringProperties = new HashMap<>();
    private String                              key;
    private String                              path;
    private Integer                             currentIndex            = -1;
    private String                              defaultValue;
    private String                              currentPropName         = null;

    public CascadingDynamicStringProperty(String path, String key, String defaultValue) {
        this.path = path;
        this.key = key;
        this.defaultValue = defaultValue;
        if (path != null && !path.isEmpty()) {
            String temp = "";
            Integer index = 0;
            for (String p : path.split("/")) {
                temp = (!temp.isEmpty() ? temp + "/" : "") + p;
                String propName = (!temp.isEmpty() ? temp + "/" : "") + key;

                class PropertyChangedEvent implements Runnable {
                    Integer index;
                    public PropertyChangedEvent(Integer index) {
                        this.index = index;
                    }
                    public void run() {
                        propertyChanged(index);
                    }
                }
                DynamicStringProperty property = DynamicPropertyFactory.getInstance().getStringProperty(propName, "default", new PropertyChangedEvent(index));
                if (property.getDynamicProperty().getCachedValue(String.class).isPresent()) {
                    this.currentIndex = index;
                    this.currentPropName = property.getName();
                }
                dynamicStringProperties.put(index, property);
                index++;
            }
        }
    }

    private void propertyChanged(Integer index) {
        // update parent key so ignore
        if (index < currentIndex) {
            return;
        }

        DynamicStringProperty property = dynamicStringProperties.get(index);
        // update child key so set the currentIndex to child index
        if (index > currentIndex) {
            if (property.getDynamicProperty().getCachedValue(String.class).isPresent()) {
                this.currentIndex = index;
                this.currentPropName = dynamicStringProperties.get(index).getDynamicProperty().getName();
                return;
            }
        }
        // this maybe a deletion of current key so we must cascade back and get the correct data
        if (index.equals(currentIndex)) {
            if (!property.getDynamicProperty().getCachedValue(String.class).isPresent()) {
                while (--index >= 0) {
                    property = dynamicStringProperties.get(index);
                    if (property.getDynamicProperty().getCachedValue(String.class).isPresent()) {
                        this.currentIndex = index;
                        this.currentPropName = property.getName();
                        return;
                    }
                }
            } else {
                return;
            }
        }
        // no property found so set currentIndex to -1
        this.currentIndex = -1;
        this.currentPropName = null;
    }

    public String get() {
        DynamicStringProperty dynamicStringProperty = dynamicStringProperties.get(currentIndex);
        return dynamicStringProperty != null ? dynamicStringProperty.get() : defaultValue;
    }

    public String getKey() {
        return key;
    }

    public String getPath() {
        return path;
    }

    public String getCurrentPropName() {
        return currentPropName;
    }
}
