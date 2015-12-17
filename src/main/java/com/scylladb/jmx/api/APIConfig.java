package com.scylladb.jmx.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

public class APIConfig {
    static String address = "localhost";
    static String port = "10000";

    public static String getAddress() {
        return address;
    }

    public static String getPort() {
        return port;
    }

    public static String getBaseUrl() {
        return "http://" + address + ":"
                + port;
    }

    public static void readFile(String name) {
        System.out.println("Using config file: " + name);
        InputStream input;
        try {
            input = new FileInputStream(new File(name));
            Yaml yaml = new Yaml();
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) yaml.load(input);
            if (map.containsKey("listen_address")) {
                address = (String) map.get("listen_address");
            }
            if (map.containsKey("api_address")) {
                address = (String) map.get("api_address");
            }
            if (map.containsKey("api_port")) {
                port = (String) map.get("api_port").toString();
            }
        } catch (FileNotFoundException e) {
            System.err.println("fail reading from config file: " + name);
            System.exit(-1);
        }
    }

    public static boolean fileExists(String name) {
        File varTmpDir = new File(name);
        return varTmpDir.exists();
    }

    public static boolean loadIfExists(String path, String name) {
        if (path == null) {
            return false;
        }
        if (!fileExists(path + name)) {
            return false;
        }
        readFile(path + name);
        return true;
    }
    /**
     * setConfig load the JMX proxy configuration
     * The configuration hierarchy is as follow:
     * Command line argument takes precedence over everything
     * Then configuration file in the command line (command line
     * argument can replace specific values in it.
     * Then SCYLLA_CONF/scylla.yaml
     * Then SCYLLA_HOME/conf/scylla.yaml
     * Then conf/scylla.yaml
     * Then the default values
     * With file configuration, to make it clearer what is been used, only
     * one file will be chosen with the highest precedence
     */
    public static void setConfig() {
        if (!System.getProperty("apiconfig","").equals("")) {
            readFile(System.getProperty("apiconfig"));
        } else if (!loadIfExists(System.getenv("SCYLLA_CONF"), "/scylla.yaml") &&
            !loadIfExists(System.getenv("SCYLLA_HOME"), "/conf/scylla.yaml")) {
            loadIfExists("", "conf/scylla.yaml");
        }

        if (!System.getProperty("apiaddress", "").equals("")) {
            address = System.getProperty("apiaddress");
        }
        if (!System.getProperty("apiport", "").equals("")) {
            port = System.getProperty("apiport", "10000");
        }
    }
}
