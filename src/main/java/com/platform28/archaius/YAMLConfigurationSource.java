package com.platform28.archaius;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.config.PollResult;
import com.netflix.config.PolledConfigurationSource;
import com.platform28.jackson.module.SqlDateAsISO8601;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

public class YAMLConfigurationSource implements PolledConfigurationSource {

    private String filePath;

    private ObjectMapper yamlObjectMapper;

    public YAMLConfigurationSource(String filePath) {
        this.filePath = filePath;
        this.yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        this.yamlObjectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.yamlObjectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        this.yamlObjectMapper.registerModule(new SqlDateAsISO8601());
        this.yamlObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public PollResult poll(boolean b, Object o) throws Exception {
        Map<String, Object> configurations = new LinkedHashMap();
        File f = new File(filePath);
        if (f.isFile()) {
            InputStream in     = new FileInputStream(f);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String         config = "";
            String line;
            while ((line = reader.readLine()) != null) {
                config += line + "\n";
            }

            Map<String, Object> configs = this.yamlObjectMapper.readValue(config, Map.class);
            configurations.putAll(configs);
        }
        return PollResult.createFull(configurations);
    }
}
