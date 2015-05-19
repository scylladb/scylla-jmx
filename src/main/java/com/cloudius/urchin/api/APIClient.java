/*
 * Copyright 2015 Cloudius Systems
 */
package com.cloudius.urchin.api;

import java.io.StringReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonString;
import javax.management.openmbean.TabularData;
import javax.ws.rs.core.UriBuilder;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import javax.ws.rs.core.MediaType;

public class APIClient {
    JsonReaderFactory factory = Json.createReaderFactory(null);

    public static String getBaseUrl() {
        return "http://" + System.getProperty("apiaddress", "localhost") + ":"
                + System.getProperty("apiport", "10000");
    }

    public Builder get(String path) {
        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        WebResource service = client.resource(UriBuilder.fromUri(getBaseUrl())
                .build());
        return service.path(path).accept(MediaType.APPLICATION_JSON);
    }

    public String getStringValue(String string) {
        if (string != "") {
            return get(string).get(String.class);
        }
        return "";
    }

    public JsonReader getReader(String string) {
        return factory.createReader(new StringReader(getStringValue(string)));
    }

    public String[] getStringArrValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public int getIntValue(String string) {
        return Integer.parseInt(getStringValue(string));
    }

    public boolean getBooleanValue(String string) {
        return Boolean.parseBoolean(getStringValue(string));
    }

    public double getDoubleValue(String string) {
        return Double.parseDouble(getStringValue(string));
    }

    public List<String> getListStrValue(String string) {
        JsonReader reader = getReader(string);
        JsonArray arr = reader.readArray();
        List<String> res = new ArrayList<String>(arr.size());
        for (int i = 0; i < arr.size(); i++) {
            res.add(arr.getString(i));
        }
        reader.close();
        return res;

    }

    public Map<List<String>, List<String>> getMapListStrValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<String, String> getMapStrValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<InetAddress> getListInetAddressValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<String, TabularData> getMapStringTabularDataValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public long getLongValue(String string) {
        return Long.parseLong(getStringValue(string));
    }

    public Map<InetAddress, Float> getMapInetAddressFloatValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<String, Long> getMapStringLongValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public long[] getLongArrValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<String, Integer> getMapStringIntegerValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public int[] getIntArrValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<String, Long> getListMapStringLongValue(String string) {
        if (string.equals("")) {
            return null;
        }
        // Builder builder =

        String vals = get(string).get(String.class);
        System.out.println(vals);
        JsonReader reader = getReader(string);
        JsonArray arr = reader.readArray();
        System.out.println(arr.size());
        Map<String, Long> map = new HashMap<String, Long>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            Iterator<String> it = obj.keySet().iterator();
            String key = "";
            long val = -1;
            while (it.hasNext()) {
                String k = (String) it.next();
                System.out.println(k);
                if (obj.get(k) instanceof JsonString) {
                    key = obj.getString(k);
                } else {
                    val = obj.getInt(k);
                }
            }
            if (val > 0 && !key.equals("")) {
                map.put(key, val);
            }

        }
        reader.close();

        // .get(String.class);

        return map;
    }
}
