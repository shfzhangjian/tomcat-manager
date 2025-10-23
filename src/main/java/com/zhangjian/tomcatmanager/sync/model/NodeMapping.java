package com.zhangjian.tomcatmanager.sync.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Represents the configuration for mapping data from a source table
 * to a specific type of node in Neo4j.
 */
public class NodeMapping {

    /**
     * A unique key identifying this specific mapping step/definition within the YAML.
     * Not directly used in Cypher, but helpful for logic in EamSyncService.
     */
    private String nodeKey;

    /**
     * The primary label for the Neo4j node (e.g., "设备", "机组").
     */
    private String label;

    /**
     * The source Oracle table name.
     */
    private String source;

    /**
     * The column name in the source Oracle table that serves as the primary key
     * for merging/identifying nodes in Neo4j.
     */
    private String primaryKey;

    /**
     * A map defining the mapping between Neo4j node property names (key)
     * and Oracle source table column names (value).
     * Example: {"名称": "SFNAME", "位置编号": "SFCODE"}
     */
    private Map<String, String> properties;

    /**
     * Configuration for looking up additional dynamic labels based on a column value.
     */
    private DynamicLabelLookup dynamicLabelLookup;

    // --- Getters and Setters ---

    // Using JsonIgnore for nodeKey as it's primarily for internal service logic, not part of the core mapping structure parsed directly for Cypher build in the simplest sense.
    @JsonIgnore
    public String getNodeKey() {
        return nodeKey;
    }

    public void setNodeKey(String nodeKey) {
        this.nodeKey = nodeKey;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public DynamicLabelLookup getDynamicLabelLookup() {
        return dynamicLabelLookup;
    }

    public void setDynamicLabelLookup(DynamicLabelLookup dynamicLabelLookup) {
        this.dynamicLabelLookup = dynamicLabelLookup;
    }

    /**
     * Inner class to hold configuration for dynamic label lookups.
     */
    public static class DynamicLabelLookup {
        /**
         * The name of the Oracle table used for the lookup (e.g., FUNPTYPE).
         */
        private String lookupTable;
        /**
         * The column name in the primary source table (e.g., EQTREE.SFTYPE)
         * whose value will be used as the key for the lookup.
         */
        private String keyColumn;
        /**
         * The column name in the lookup table (e.g., FUNPTYPE.STYNAME)
         * that contains the dynamic label string.
         */
        private String labelColumn;
        /**
         * The column name in the lookup table (e.g., FUNPTYPE.SFTYPE)
         * used to match against the value from keyColumn.
         */
        private String lookupKeyColumn;


        // --- Getters and Setters ---

        public String getLookupTable() {
            return lookupTable;
        }

        public void setLookupTable(String lookupTable) {
            this.lookupTable = lookupTable;
        }

        public String getKeyColumn() {
            return keyColumn;
        }

        public void setKeyColumn(String keyColumn) {
            this.keyColumn = keyColumn;
        }

        public String getLabelColumn() {
            return labelColumn;
        }

        public void setLabelColumn(String labelColumn) {
            this.labelColumn = labelColumn;
        }

        public String getLookupKeyColumn() {
            return lookupKeyColumn;
        }

        public void setLookupKeyColumn(String lookupKeyColumn) {
            this.lookupKeyColumn = lookupKeyColumn;
        }
    }
}

