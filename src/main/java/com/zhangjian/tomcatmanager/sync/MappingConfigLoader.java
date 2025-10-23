package com.zhangjian.tomcatmanager.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.zhangjian.tomcatmanager.sync.model.MappingConfig;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class MappingConfigLoader {

    private final ObjectMapper objectMapper;

    public MappingConfigLoader() {
        // FIX: Initialize ObjectMapper with a YAMLFactory to parse YAML content
        this.objectMapper = new ObjectMapper(new YAMLFactory());
        this.objectMapper.findAndRegisterModules();
    }

    /**
     * Loads and parses the YAML mapping configuration from a string.
     * @param yamlContent The string containing the YAML configuration.
     * @return A MappingConfig object.
     * @throws IOException If parsing fails.
     */
    public MappingConfig load(String yamlContent) throws IOException {
        if (yamlContent == null || yamlContent.trim().isEmpty()) {
            throw new IOException("Mapping configuration content cannot be null or empty.");
        }
        return objectMapper.readValue(yamlContent, MappingConfig.class);
    }
}

