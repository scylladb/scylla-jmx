package com.scylladb.jmx.metrics;

import static com.scylladb.jmx.metrics.RegistrationMode.Remove;
import static com.scylladb.jmx.metrics.RegistrationMode.Wait;
import static java.util.EnumSet.allOf;
import static java.util.EnumSet.of;

import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.OperationsException;

import com.scylladb.jmx.api.APIClient;
import com.sun.jmx.mbeanserver.JmxMBeanServer;

/**
 * Helper type to do optional locking for registration. Allows for
 * per-bind-point locks and registration, instead of per-type or per-instance
 * locks which may be misguiding, since for example one instance can be bound to
 * many MBeanServers etc.
 * 
 * Also allows for polled checks, i.e. try-lock and either wait or skip. Wait,
 * because we probably should not repeat things hidden by this type too often,
 * and skip because for example a periodic task checking can just skip if a
 * user-initiated registration check is being done.
 * 
 * @author calle
 *
 */
@SuppressWarnings("restriction")
public abstract class RegistrationChecker {
    private final Lock lock = new ReentrantLock();

    public static final EnumSet<RegistrationMode> REMOVE_NO_WAIT = of(Remove);
    public static final EnumSet<RegistrationMode> ADD_AND_REMOVE = allOf(RegistrationMode.class);

    public final void reap(APIClient client, JmxMBeanServer server) throws OperationsException, UnknownHostException {
        check(client, server, REMOVE_NO_WAIT);
    }

    public final void check(APIClient client, JmxMBeanServer server) throws OperationsException, UnknownHostException {
        check(client, server, ADD_AND_REMOVE);
    }

    public final void check(APIClient client, JmxMBeanServer server, EnumSet<RegistrationMode> mode)
            throws OperationsException, UnknownHostException {
        if (!lock.tryLock()) {
            if (mode.contains(Wait)) {
                // someone is doing update.
                // since this is jmx, and sloppy, we'll just
                // assume that once he is done, things are
                // good enough.
                lock.lock();
                lock.unlock();
            }
            return;
        }
        try {
            doCheck(client, server, mode);
        } finally {
            lock.unlock();
        }
    }

    protected abstract void doCheck(APIClient client, JmxMBeanServer server, EnumSet<RegistrationMode> mode)
            throws OperationsException, UnknownHostException;
}
