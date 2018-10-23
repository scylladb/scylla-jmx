package com.scylladb.jmx.utils;

import java.io.ObjectInputStream;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.StreamingMetrics;

import com.scylladb.jmx.api.APIClient;
import com.sun.jmx.mbeanserver.JmxMBeanServer;

public class APIMBeanServer implements MBeanServer {
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(APIMBeanServer.class.getName());

    private final APIClient client;
    private final JmxMBeanServer server;

    public APIMBeanServer(APIClient client, JmxMBeanServer server) {
        this.client = client;
        this.server = server;
    }

    private static ObjectInstance prepareForRemote(final ObjectInstance i) {
        return new ObjectInstance(prepareForRemote(i.getObjectName()), i.getClassName());
    }

    private static ObjectName prepareForRemote(final ObjectName n) {
        /*
         * ObjectName.getInstance has changed in JDK (micro) updates so it no longer applies 
         * overridable methods -> wrong name published. 
         * Fix by doing explicit ObjectName instansiation. 
         */
        try {
            return new ObjectName(n.getCanonicalName());
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(n.toString());
        }
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException,
            InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
        return prepareForRemote(server.createMBean(className, name));
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
            NotCompliantMBeanException, InstanceNotFoundException {
        return prepareForRemote(server.createMBean(className, name, loaderName));
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
            NotCompliantMBeanException {
        return prepareForRemote(server.createMBean(className, name, params, signature));
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params,
            String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
                    MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
        return prepareForRemote(server.createMBean(className, name, loaderName, params, signature));
    }

    @Override
    public ObjectInstance registerMBean(Object object, ObjectName name)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        return prepareForRemote(server.registerMBean(object, name));
    }

    @Override
    public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
        server.unregisterMBean(name);
    }

    @Override
    public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
        checkRegistrations(name);
        return prepareForRemote(server.getObjectInstance(name));
    }

    @Override
    public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
        checkRegistrations(name);
        return server.queryNames(name, query).stream().map(n -> prepareForRemote(n)).collect(Collectors.toSet());
    }

    @Override
    public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
        checkRegistrations(name);
        return server.queryMBeans(name, query).stream().map(i -> prepareForRemote(i)).collect(Collectors.toSet());
    }

    @Override
    public boolean isRegistered(ObjectName name) {
        checkRegistrations(name);
        return server.isRegistered(name);
    }

    @Override
    public Integer getMBeanCount() {
        return server.getMBeanCount();
    }

    @Override
    public Object getAttribute(ObjectName name, String attribute)
            throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
        checkRegistrations(name);
        return server.getAttribute(name, attribute);
    }

    @Override
    public AttributeList getAttributes(ObjectName name, String[] attributes)
            throws InstanceNotFoundException, ReflectionException {
        checkRegistrations(name);
        return server.getAttributes(name, attributes);
    }

    @Override
    public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException,
            AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        checkRegistrations(name);
        server.setAttribute(name, attribute);
    }

    @Override
    public AttributeList setAttributes(ObjectName name, AttributeList attributes)
            throws InstanceNotFoundException, ReflectionException {
        checkRegistrations(name);
        return server.setAttributes(name, attributes);
    }

    @Override
    public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
            throws InstanceNotFoundException, MBeanException, ReflectionException {
        checkRegistrations(name);
        return server.invoke(name, operationName, params, signature);
    }

    @Override
    public String getDefaultDomain() {
        return server.getDefaultDomain();
    }

    @Override
    public String[] getDomains() {
        return server.getDomains();
    }

    @Override
    public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
            Object handback) throws InstanceNotFoundException {
        server.addNotificationListener(name, listener, filter, handback);
    }

    @Override
    public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
            Object handback) throws InstanceNotFoundException {
        server.addNotificationListener(name, listener, filter, handback);
    }

    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener)
            throws InstanceNotFoundException, ListenerNotFoundException {
        server.removeNotificationListener(name, listener);
    }

    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
            Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        server.removeNotificationListener(name, listener, filter, handback);
    }

    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener)
            throws InstanceNotFoundException, ListenerNotFoundException {
        server.removeNotificationListener(name, listener);
    }

    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
            Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        server.removeNotificationListener(name, listener, filter, handback);
    }

    @Override
    public MBeanInfo getMBeanInfo(ObjectName name)
            throws InstanceNotFoundException, IntrospectionException, ReflectionException {
        checkRegistrations(name);
        return server.getMBeanInfo(name);
    }

    @Override
    public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException {
        return server.isInstanceOf(name, className);
    }

    @Override
    public Object instantiate(String className) throws ReflectionException, MBeanException {
        return server.instantiate(className);
    }

    @Override
    public Object instantiate(String className, ObjectName loaderName)
            throws ReflectionException, MBeanException, InstanceNotFoundException {
        return server.instantiate(className, loaderName);
    }

    @Override
    public Object instantiate(String className, Object[] params, String[] signature)
            throws ReflectionException, MBeanException {
        return server.instantiate(className, params, signature);
    }

    @Override
    public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature)
            throws ReflectionException, MBeanException, InstanceNotFoundException {
        return server.instantiate(className, loaderName, params, signature);
    }

    @Override
    @Deprecated
    public ObjectInputStream deserialize(ObjectName name, byte[] data)
            throws InstanceNotFoundException, OperationsException {
        return server.deserialize(name, data);
    }

    @Override
    @Deprecated
    public ObjectInputStream deserialize(String className, byte[] data)
            throws OperationsException, ReflectionException {
        return server.deserialize(className, data);
    }

    @Override
    @Deprecated
    public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
            throws InstanceNotFoundException, OperationsException, ReflectionException {
        return server.deserialize(className, loaderName, data);
    }

    @Override
    public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
        return server.getClassLoaderFor(mbeanName);
    }

    @Override
    public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException {
        return server.getClassLoader(loaderName);
    }

    @Override
    public ClassLoaderRepository getClassLoaderRepository() {
        return server.getClassLoaderRepository();
    }

    static final Pattern tables = Pattern.compile("^(ColumnFamil(ies|y)|(Index)?Tables?)$");

    private boolean checkRegistrations(ObjectName name) {
        if (name != null && server.isRegistered(name)) {
            return false;
        }
        
        boolean result = false;
        
        try {
            String type = name != null ? name.getKeyProperty("type") : null;            
            if (type == null || tables.matcher(type).matches()) {
                result |= ColumnFamilyStore.checkRegistration(client, server);
            }
            if (type == null || StreamingMetrics.TYPE_NAME.equals(type)) {
                result |= StreamingMetrics.checkRegistration(client, server);
            }
        } catch (MalformedObjectNameException | UnknownHostException e) {
            // TODO: log
        }
        return result;
    }
}