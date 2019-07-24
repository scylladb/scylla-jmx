package org.apache.cassandra.service;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scylladb.jmx.utils.DateXmlAdapter;

public class SSTableInfo {
    private long size;

    @JsonProperty("data_size")
    private long dataSize;

    @JsonProperty("index_size")
    private long indexSize;

    @JsonProperty("filter_size")
    private long filterSize;

    @XmlJavaTypeAdapter(type = Date.class, value = DateXmlAdapter.class)
    private Date timestamp;

    private long generation;

    private long level;

    private String version;

    private Map<String, String> properties;

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    private Map<String, Map<String, String>> extendedProperties;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public long getIndexSize() {
        return indexSize;
    }

    public void setIndexSize(long indexSize) {
        this.indexSize = indexSize;
    }

    public long getFilterSize() {
        return filterSize;
    }

    public void setFilterSize(long filterSize) {
        this.filterSize = filterSize;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(long generation) {
        this.generation = generation;
    }

    public long getLevel() {
        return level;
    }

    public void setLevel(long level) {
        this.level = level;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, Map<String, String>> getExtendedProperties() {
        return extendedProperties;
    }

    public void setExtendedProperties(Map<String, Map<String, String>> extendedProperties) {
        this.extendedProperties = extendedProperties;
    }

    @JsonProperty("extended_properties")
    private void unpackNested(List<Map<String, Object>> properties) {
        Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();

        for (Map<String, Object> map : properties) {
            Object name = map.get("group");
            if (name != null) {
                Map<String, String> dst = new HashMap<>();
                List<?> value = (List<?>) map.get("attributes");
                for (Object v : value) {
                    Map<?, ?> subMap = (Map<?, ?>) v;
                    dst.put(String.valueOf(subMap.get("key")), String.valueOf(subMap.get("value")));
                }
                result.put(String.valueOf(name), dst);
            } else {
                for (Map.Entry<String, Object> e : map.entrySet()) {
                    result.put(e.getKey(), Collections.singletonMap(String.valueOf(e.getValue()), ""));
                }
            }
        }
        extendedProperties = result;
    }
}
