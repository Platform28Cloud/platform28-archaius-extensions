package com.platform28.archaius;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.config.PollResult;
import com.netflix.config.PolledConfigurationSource;
import com.platform28.jackson.module.SqlDateAsISO8601;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

import java.io.File;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class YamlConfigurationSource implements PolledConfigurationSource {
    private static final Log    logger = LogFactory.getLog(YamlConfigurationSource.class);

    private          String filePath;
    private          String encryptionPassword;

    private ObjectMapper               yamlObjectMapper;
    private ObjectMapper               jsonObjectMapper;
    private StandardPBEStringEncryptor encryptor;
    private static final String SECURE_PREFIX = "::secure::";

    public YamlConfigurationSource(String filePath, String encryptionPassword) {
        this.filePath = filePath;
        this.encryptionPassword = encryptionPassword;
        this.encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(this.encryptionPassword);
        this.jsonObjectMapper = new ObjectMapper(new JsonFactory());
        this.yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        this.yamlObjectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.yamlObjectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        this.yamlObjectMapper.registerModule(new SqlDateAsISO8601());
        this.yamlObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public PollResult poll(boolean b, Object o) throws Exception {
        Map<String, Object> configurations = new LinkedHashMap();
        try {
            File f = new File(filePath);
            if (f.isFile()) {
                byte[] bytes = Files.readAllBytes(f.toPath());
                String config = new String(bytes);

                Map<String, Object> yamlConfigs = this.yamlObjectMapper.readValue(config, Map.class);
                Map<String, Object> propertyConfigs = new HashMap<>();

                decryptAndConvertToPropertyConfigs(yamlConfigs, null, propertyConfigs);
                configurations.putAll(propertyConfigs);
            }
        } catch (Exception ex) {
            logger.warn("Error processing configuration file " + filePath, ex);
        }
        return PollResult.createFull(configurations);
    }

    // this function will navigate through every nodes of the tree
    // and check if a property is a String and prefixed with ::secure::
    // it will be decrypted using the encryptionPassword
    // and convert yaml config structure to a property config structure
    private void decryptAndConvertToPropertyConfigs(Object property, String currentKey, Map<String, Object> propertyConfigs) {
        String nextKey = "";
        if (currentKey != null && !currentKey.isEmpty()) {
            nextKey = currentKey + ".";
        }
        if (property instanceof Map) {
            // if property is a map then we will loop thru its entries
            Map<String, Object> map = (Map<String, Object>) property;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof Map) {
                    // if entry's value is a Map then we will call this function again with this map
                    decryptAndConvertToPropertyConfigs(value, nextKey + entry.getKey(), propertyConfigs);
                } else if (value instanceof List) {
                    // if entry's value is a List then we will call this function again with this list
                    decryptAndConvertToPropertyConfigs(value, nextKey + entry.getKey(), propertyConfigs);
                } else if (value instanceof String && ((String) value).startsWith(SECURE_PREFIX)) {
                    // if entry's value is a String and prefixed with ::secure:: then we will decrypt it
                    try {
                        value = ((String) value).substring(SECURE_PREFIX.length());
                        value = encryptor.decrypt((String) value);
                        entry.setValue(value);
                        // if propertyConfigs is not null then we add it
                        if (propertyConfigs != null) {
                            propertyConfigs.put(nextKey + entry.getKey(), value);
                        }
                    } catch (Exception ex) {
                        logger.warn("Cannot decrypt value '" + value + "' from key " + nextKey + entry.getKey());
                    }
                } else {
                    // if propertyConfigs is not null then we add it
                    if (propertyConfigs != null) {
                        propertyConfigs.put(nextKey + entry.getKey(), value);
                    }
                }
            }
        } else if (property instanceof List) {
            // if property is a List then we will loop thru its items
            Iterator i = ((List) property).iterator();
            // this temp will contain the data that has been decrypted if any
            List<String> temp = new ArrayList<>();
            while(i.hasNext()) {
                Object item = i.next();
                if (item instanceof Map) {
                    // if item is a Map then we will call this function again with this map
                    // we will transfer this array to json so we don't need to pass propertyConfigs to this
                    decryptAndConvertToPropertyConfigs(item, currentKey, null);
                } else if (item instanceof List) {
                    // if item is a List then we will call this function again with this list
                    // we will transfer this array to json so we don't need to pass propertyConfigs to this
                    decryptAndConvertToPropertyConfigs(item, currentKey, null);
                } else if (item instanceof String && ((String) item).startsWith(SECURE_PREFIX)) {
                    // if item is a String and prefixed with ::secure:: then we will decrypt it
                    try {
                        item = ((String) item).substring(SECURE_PREFIX.length());
                        item = encryptor.decrypt((String) item);
                        // remove this item from the List and add it to the temp for re-add later
                        i.remove();
                        temp.add((String) item);
                    } catch (Exception ex) {
                        logger.warn("Cannot decrypt value '" + item + "' from key " + currentKey);
                    }
                }
            }
            // add back the decrypted item
            ((List) property).addAll(temp);
            // after we process everything in the list, we will convert the list to json and add it to the propertyConfigs map
            try {
                String json = jsonObjectMapper.writeValueAsString(property);
                if (propertyConfigs != null) {
                    propertyConfigs.put(currentKey, json);
                }
            } catch (JsonProcessingException e) {
                logger.warn("Cannot convert array to json from key " + currentKey, e);
            }
        }
    }
}
