package com.platform28.data;

import org.apache.commons.configuration.AbstractConfiguration;

public class PriorityConfigPair {
    private Integer priority;
    private AbstractConfiguration abstractConfiguration;

    public PriorityConfigPair(Integer priority, AbstractConfiguration abstractConfiguration) {
        this.priority = priority;
        this.abstractConfiguration = abstractConfiguration;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public AbstractConfiguration getAbstractConfiguration() {
        return abstractConfiguration;
    }

    public void setAbstractConfiguration(AbstractConfiguration abstractConfiguration) {
        this.abstractConfiguration = abstractConfiguration;
    }
}
