package com.zhangjian.tomcatmanager.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.zhangjian.tomcatmanager.sync.model.MappingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class to load and parse the YAML mapping configuration.
 */
@Component
public class MappingConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(MappingConfigLoader.class);
    private final ObjectMapper yamlMapper;

    public MappingConfigLoader() {
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
        // Configure the mapper if necessary, e.g., to handle specific date formats
    }

    /**
     * Loads the mapping configuration from a given file path.
     *
     * @param filePath The path to the YAML configuration file.
     * @return The parsed MappingConfig object.
     * @throws IOException If the file cannot be read or parsed.
     */
    public MappingConfig loadFromFile(Path filePath) throws IOException {
        logger.info("Loading mapping configuration from file: {}", filePath);
        if (!Files.exists(filePath)) {
            logger.error("Mapping configuration file not found: {}", filePath);
            throw new IOException("Mapping configuration file not found: " + filePath);
        }
        try (InputStream inputStream = Files.newInputStream(filePath)) {
            return yamlMapper.readValue(inputStream, MappingConfig.class);
        } catch (IOException e) {
            logger.error("Failed to load or parse mapping configuration from {}: {}", filePath, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Loads the mapping configuration directly from a YAML string.
     *
     * @param yamlContent The YAML configuration as a string.
     * @return The parsed MappingConfig object.
     * @throws IOException If the string cannot be parsed.
     */
    public MappingConfig loadFromString(String yamlContent) throws IOException {
        logger.info("Loading mapping configuration from string content.");
        if (yamlContent == null || yamlContent.trim().isEmpty()) {
            logger.error("Mapping configuration string content is empty or null.");
            throw new IOException("Mapping configuration string content is empty or null.");
        }
        try {
            return yamlMapper.readValue(yamlContent, MappingConfig.class);
        } catch (IOException e) {
            logger.error("Failed to parse mapping configuration from string: {}", e.getMessage(), e);
            throw e;
        }
    }
}
