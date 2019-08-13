package org.apache.cassandra.service;

import static com.sun.jmx.mbeanserver.MXBeanMappingFactory.DEFAULT;

import java.io.InvalidObjectException;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.xml.bind.annotation.XmlRootElement;

import com.sun.jmx.mbeanserver.MXBeanMapping;

@SuppressWarnings("restriction")
@XmlRootElement
public class PerTableSSTableInfo {
    private static final MXBeanMapping mxBeanMapping;

    static {
        try {
            mxBeanMapping = DEFAULT.mappingForType(PerTableSSTableInfo.class, DEFAULT);
        } catch (OpenDataException e) {
            throw new RuntimeException(e);
        }
    }

    private String keyspace;
    private List<SSTableInfo> sstables;
    private String table;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public List<SSTableInfo> getSSTables() {
        return sstables;
    }

    public void setSSTableInfos(List<SSTableInfo> sstableInfos) {
        this.sstables = sstableInfos;
    }

    public CompositeData toCompositeData() {
        try {
            return (CompositeData) mxBeanMapping.toOpenValue(this);
        } catch (OpenDataException e) {
            throw new Error(e); // should not reach.
        }
    }

    public static PerTableSSTableInfo from(CompositeData data) throws InvalidObjectException {
        return (PerTableSSTableInfo) mxBeanMapping.fromOpenValue(data);
    }
}
