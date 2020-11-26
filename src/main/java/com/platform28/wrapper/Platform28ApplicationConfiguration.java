package com.platform28.wrapper;

import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.ConfigurationManager;
import com.platform28.data.PriorityConfigPair;
import org.apache.commons.configuration.AbstractConfiguration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Platform28ApplicationConfiguration {
    public static List<PriorityConfigPair> configurationSources = new ArrayList<>();

    public static void addConfiguration(AbstractConfiguration configuration, Integer priority) {
        ConcurrentCompositeConfiguration concurrentCompositeConfiguration;
        if (!ConfigurationManager.isConfigurationInstalled()) {
            concurrentCompositeConfiguration = new ConcurrentCompositeConfiguration();
            ConfigurationManager.install(concurrentCompositeConfiguration);
        } else {
            concurrentCompositeConfiguration = (ConcurrentCompositeConfiguration) ConfigurationManager.getConfigInstance();
        }
        // clean up old configurations
        concurrentCompositeConfiguration.clear();

        PriorityConfigPair priorityConfigPair = new PriorityConfigPair(priority, configuration);
        configurationSources.add(priorityConfigPair);
        configurationSources.sort(Comparator.comparingInt(PriorityConfigPair::getPriority));
        for (PriorityConfigPair p : configurationSources) {
            concurrentCompositeConfiguration.addConfiguration(p.getAbstractConfiguration());
        }
    }
}
