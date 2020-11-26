package com.platform28.archaius;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.config.PollResult;
import com.netflix.config.PolledConfigurationSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

public class EncryptedStringConfigurationSource implements PolledConfigurationSource {
    private static final Log                        logger = LogFactory.getLog(EncryptedStringConfigurationSource.class);

    private              String                     filePath;
    private              String                     propertyName;
    private              String                     encryptionPassword;
    private              StandardPBEStringEncryptor encryptor;
    private              ObjectMapper               mapper = new ObjectMapper();

    public EncryptedStringConfigurationSource(String filePath, String propertyName, String encryptionPassword) {
        this.filePath = filePath;
        this.propertyName = propertyName;
        this.encryptionPassword = encryptionPassword;
        this.encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(this.encryptionPassword);
    }

    @Override
    public PollResult poll(boolean initial, Object checkPoint) throws IOException {
        Map<String, Object> configurations = new LinkedHashMap<>();
        try {
            File f = new File(filePath);
            if (f.isFile()) {
                byte[] bytes = Files.readAllBytes(f.toPath());
                String config = new String(bytes);
                config = encryptor.decrypt(config);
                configurations.put(propertyName, config);
            }
        } catch (Exception ex) {
            logger.warn("Error processing configuration file " + filePath, ex);
        }
        return PollResult.createFull(configurations);
    }
}
