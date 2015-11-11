/*
 * Copyright 2015 Cloudius Systems
 */
package com.cloudius.urchin.api;

import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonString;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;
import com.cloudius.urchin.utils.EstimatedHistogram;
import com.cloudius.urchin.utils.SnapshotDetailsTabularData;
import com.yammer.metrics.core.HistogramValues;

public class APIClient {
    Map<String, CacheEntry> cache = new HashMap<String, CacheEntry>();
    String getCacheKey(String key, MultivaluedMap<String, String> param, long duration) {
        if (duration <= 0) {
            return null;
        }
        if (param != null) {
            StringBuilder sb = new StringBuilder(key);
            sb.append("?");
            for (String k : param.keySet()) {
                sb.append(k).append('=').append(param.get(k)).append('&');
            }
            return sb.toString();
        }
        return key;
    }

    String getStringFromCache(String key, long duration) {
        if (key == null) {
            return null;
        }
        CacheEntry value = cache.get(key);
        return (value!= null && value.valid(duration))? value.stringValue() : null;
    }

    EstimatedHistogram getEstimatedHistogramFromCache(String key, long duration) {
        if (key == null) {
            return null;
        }
        CacheEntry value = cache.get(key);
        return (value!= null && value.valid(duration))? value.getEstimatedHistogram() : null;
    }

    JsonReaderFactory factory = Json.createReaderFactory(null);
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(APIClient.class.getName());

    public static String getBaseUrl() {
        return APIConfig.getBaseUrl();
    }

    public Invocation.Builder get(String path, MultivaluedMap<String, String> queryParams) {
        Client client =  ClientBuilder.newClient( new ClientConfig());
        WebTarget webTarget = client.target(getBaseUrl()).path(path);
        if (queryParams != null) {
            for (Entry<String, List<String>> qp :  queryParams.entrySet()) {
                for (String e : qp.getValue()) {
                    webTarget = webTarget.queryParam(qp.getKey(), e);
                }
            }
        }
        return webTarget.request(MediaType.APPLICATION_JSON);
    }

    public Invocation.Builder get(String path) {
        return get(path, null);
    }

    public Response post(String path, MultivaluedMap<String, String> queryParams) {
        Response response = get(path, queryParams).post(Entity.entity(null, MediaType.TEXT_PLAIN));
        if (response.getStatus() != Response.Status.OK.getStatusCode() ) {
            throw getException(response.readEntity(String.class));
        }
        return response;

    }

    public void post(String path) {
        post(path, null);
    }

    public RuntimeException getException(String txt) {
        JsonReader reader = factory.createReader(new StringReader(txt));
        JsonObject res = reader.readObject();
        return new RuntimeException(res.getString("message"));
    }

    public String postGetVal(String path, MultivaluedMap<String, String> queryParams) {
        return post(path, queryParams).readEntity(String.class);
    }

    public int postInt(String path, MultivaluedMap<String, String> queryParams) {
        return Integer.parseInt(postGetVal(path, queryParams));
    }

    public int postInt(String path) {
        return postInt(path, null);
    }

    public void delete(String path, MultivaluedMap<String, String> queryParams) {
        if (queryParams != null) {
            get(path, queryParams).delete();
            return;
        }
        get(path).delete();
    }

    public void delete(String path) {
        delete(path, null);
    }

    public String getRawValue(String string,
            MultivaluedMap<String, String> queryParams, long duration) {
        if (string.equals("")) {
            return "";
        }
        String key = getCacheKey(string, queryParams, duration);
        String res = getStringFromCache(key, duration);
        if (res != null) {
            return res;
        }
        Response response = get(string, queryParams).get(Response.class);

        if (response.getStatus() != Response.Status.OK.getStatusCode() ) {
            // TBD
            // We are currently not caching errors,
            // it should be reconsider.
            throw getException(response.readEntity(String.class));
        }
        res = response.readEntity(String.class);
        if (duration > 0) {
            cache.put(key, new CacheEntry(res));
        }
        return res;
    }

    public String getRawValue(String string,
            MultivaluedMap<String, String> queryParams) {
        return getRawValue(string, queryParams, 0);
    }

    public String getRawValue(String string, long duration) {
        return getRawValue(string, null, duration);
    }

    public String getRawValue(String string) {
        return getRawValue(string, null, 0);
    }

    public String getStringValue(String string, MultivaluedMap<String, String> queryParams) {
        return getRawValue(string,
                queryParams).replaceAll("^\"|\"$", "");
    }

    public String getStringValue(String string, MultivaluedMap<String, String> queryParams, long duration) {
        return getRawValue(string,
                queryParams, duration).replaceAll("^\"|\"$", "");
    }

    public String getStringValue(String string) {
        return getStringValue(string, null);
    }

    public JsonReader getReader(String string,
            MultivaluedMap<String, String> queryParams) {
        return factory.createReader(new StringReader(getRawValue(string,
                queryParams)));
    }

    public JsonReader getReader(String string) {
        return getReader(string, null);
    }

    public String[] getStringArrValue(String string) {
        List<String> val = getListStrValue(string);
        return val.toArray(new String[val.size()]);
    }

    public int getIntValue(String string,
            MultivaluedMap<String, String> queryParams) {
        return Integer.parseInt(getRawValue(string, queryParams));
    }

    public int getIntValue(String string) {
        return getIntValue(string, null);
    }

    public boolean getBooleanValue(String string) {
        return Boolean.parseBoolean(getRawValue(string));
    }

    public double getDoubleValue(String string) {
        return Double.parseDouble(getRawValue(string));
    }

    public List<String> getListStrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        List<String> res = new ArrayList<String>(arr.size());
        for (int i = 0; i < arr.size(); i++) {
            res.add(arr.getString(i));
        }
        reader.close();
        return res;

    }

    public List<String> getListStrValue(String string) {
        return getListStrValue(string, null);

    }

    public static List<String> listStrFromJArr(JsonArray arr) {
        List<String> res = new ArrayList<String>();
        for (int i = 0; i < arr.size(); i++) {
            res.add(arr.getString(i));
        }
        return res;
    }

    public static Map<String, String> mapStrFromJArr(JsonArray arr) {
        Map<String, String> res = new HashMap<String, String>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            if (obj.containsKey("key") && obj.containsKey("value")) {
                res.put(obj.getString("key"), obj.getString("value"));
            }
        }
        return res;
    }

    public static String join(String[] arr, String joiner) {
        String res = "";
        if (arr != null) {
            for (String name : arr) {
                if (name != null && !name.equals("")) {
                    if (!res.equals("")) {
                        res = res + ",";
                    }
                    res = res + name;
                }
            }
        }
        return res;
    }

    public static String join(String[] arr) {
        return join(arr, ",");
    }

    public static String mapToString(Map<String, String> mp, String pairJoin,
            String joiner) {
        String res = "";
        if (mp != null) {
            for (String name : mp.keySet()) {
                if (!res.equals("")) {
                    res = res + joiner;
                }
                res = res + name + pairJoin + mp.get(name);
            }
        }
        return res;
    }

    public static String mapToString(Map<String, String> mp) {
        return mapToString(mp, "=", ",");
    }

    public static boolean set_query_param(
            MultivaluedMap<String, String> queryParams, String key, String value) {
        if (queryParams != null && key != null && value != null
                && !value.equals("")) {
            queryParams.add(key, value);
            return true;
        }
        return false;
    }

    public static boolean set_bool_query_param(
            MultivaluedMap<String, String> queryParams, String key,
            boolean value) {
        if (queryParams != null && key != null && value) {
            queryParams.add(key, "true");
            return true;
        }
        return false;
    }

    public Map<String, List<String>> getMapStringListStrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        if (string.equals("")) {
            return null;
        }
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            if (obj.containsKey("key") && obj.containsKey("value")) {
                map.put(obj.getString("key"),
                        listStrFromJArr(obj.getJsonArray("value")));
            }
        }
        reader.close();
        return map;
    }

    public Map<String, List<String>> getMapStringListStrValue(String string) {
        return getMapStringListStrValue(string, null);
    }

    public Map<List<String>, List<String>> getMapListStrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        if (string.equals("")) {
            return null;
        }
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            if (obj.containsKey("key") && obj.containsKey("value")) {
                map.put(listStrFromJArr(obj.getJsonArray("key")),
                        listStrFromJArr(obj.getJsonArray("value")));
            }
        }
        reader.close();
        return map;
    }

    public Map<List<String>, List<String>> getMapListStrValue(String string) {
        return getMapListStrValue(string, null);
    }

    public Set<String> getSetStringValue(String string,
            MultivaluedMap<String, String> queryParams) {
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        Set<String> res = new HashSet<String>();
        for (int i = 0; i < arr.size(); i++) {
            res.add(arr.getString(i));
        }
        reader.close();
        return res;
    }

    public Set<String> getSetStringValue(String string) {
        return getSetStringValue(string, null);
    }

    public Map<String, String> getMapStrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        if (string.equals("")) {
            return null;
        }
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            if (obj.containsKey("key") && obj.containsKey("value")) {
                map.put(obj.getString("key"), obj.getString("value"));
            }
        }
        reader.close();
        return map;
    }

    public Map<String, String> getMapStrValue(String string) {
        return getMapStrValue(string, null);
    }

    public List<InetAddress> getListInetAddressValue(String string,
            MultivaluedMap<String, String> queryParams) {
        List<String> vals = getListStrValue(string, queryParams);
        List<InetAddress> res = new ArrayList<InetAddress>();
        for (String val : vals) {
            try {
                res.add(InetAddress.getByName(val));
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return res;
    }

    public List<InetAddress> getListInetAddressValue(String string) {
        return getListInetAddressValue(string, null);
    }

    public Map<String, TabularData> getMapStringTabularDataValue(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    private TabularDataSupport getSnapshotData(String ks, JsonArray arr) {
        TabularDataSupport data = new TabularDataSupport(
                SnapshotDetailsTabularData.TABULAR_TYPE);

        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            if (obj.containsKey("key") && obj.containsKey("cf")) {
                SnapshotDetailsTabularData.from(obj.getString("key"), ks,
                        obj.getString("cf"), obj.getInt("total"),
                        obj.getInt("live"), data);
            }
        }
        return data;
    }

    public Map<String, TabularData> getMapStringSnapshotTabularDataValue(
            String string, MultivaluedMap<String, String> queryParams) {
        if (string.equals("")) {
            return null;
        }
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        Map<String, TabularData> map = new HashMap<>();

        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            if (obj.containsKey("key") && obj.containsKey("value")) {
                String key = obj.getString("key");
                map.put(key, getSnapshotData(key, obj.getJsonArray("value")));
            }
        }
        reader.close();
        return map;
    }

    public long getLongValue(String string) {
        return Long.parseLong(getRawValue(string));
    }

    public Map<InetAddress, Float> getMapInetAddressFloatValue(String string,
            MultivaluedMap<String, String> queryParams) {
        Map<InetAddress, Float> res = new HashMap<InetAddress, Float>();

        JsonReader reader = getReader(string, queryParams);

        JsonArray arr = reader.readArray();
        JsonObject obj = null;
        for (int i = 0; i < arr.size(); i++) {
            try {
                obj = arr.getJsonObject(i);
                res.put(InetAddress.getByName(obj.getString("key")),
                        Float.parseFloat(obj.getString("value")));
            } catch (UnknownHostException e) {
                logger.warning("Bad formatted address " + obj.getString("key"));
            }
        }
        return res;
    }

    public Map<InetAddress, Float> getMapInetAddressFloatValue(String string) {
        return getMapInetAddressFloatValue(string, null);
    }

    public Map<String, Long> getMapStringLongValue(String string,
            MultivaluedMap<String, String> queryParams) {
        Map<String, Long> res = new HashMap<String, Long>();

        JsonReader reader = getReader(string, queryParams);

        JsonArray arr = reader.readArray();
        JsonObject obj = null;
        for (int i = 0; i < arr.size(); i++) {
            obj = arr.getJsonObject(i);
            res.put(obj.getString("key"), obj.getJsonNumber("value").longValue());
        }
        return res;
    }

    public Map<String, Long> getMapStringLongValue(String string) {
        return getMapStringLongValue(string, null);
    }

    public long[] getLongArrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        long[] res = new long[arr.size()];
        for (int i = 0; i < arr.size(); i++) {
            res[i] = arr.getJsonNumber(i).longValue();
        }
        reader.close();
        return res;
    }

    public long[] getLongArrValue(String string) {
        return getLongArrValue(string, null);
    }

    public Map<String, Integer> getMapStringIntegerValue(String string,
            MultivaluedMap<String, String> queryParams) {
        Map<String, Integer> res = new HashMap<String, Integer>();

        JsonReader reader = getReader(string, queryParams);

        JsonArray arr = reader.readArray();
        JsonObject obj = null;
        for (int i = 0; i < arr.size(); i++) {
            obj = arr.getJsonObject(i);
            res.put(obj.getString("key"), obj.getInt("value"));
        }
        return res;
    }

    public Map<String, Integer> getMapStringIntegerValue(String string) {
        return getMapStringIntegerValue(string, null);
    }

    public int[] getIntArrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        int[] res = new int[arr.size()];
        for (int i = 0; i < arr.size(); i++) {
            res[i] = arr.getInt(i);
        }
        reader.close();
        return res;
    }

    public int[] getIntArrValue(String string) {
        return getIntArrValue(string, null);
    }

    public Map<String, Long> getListMapStringLongValue(String string,
            MultivaluedMap<String, String> queryParams) {
        if (string.equals("")) {
            return null;
        }
        JsonReader reader = getReader(string, queryParams);
        JsonArray arr = reader.readArray();
        Map<String, Long> map = new HashMap<String, Long>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            Iterator<String> it = obj.keySet().iterator();
            String key = "";
            long val = -1;
            while (it.hasNext()) {
                String k = it.next();
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
        return map;
    }

    public Map<String, Long> getListMapStringLongValue(String string) {
        return getListMapStringLongValue(string, null);
    }

    public JsonArray getJsonArray(String string,
            MultivaluedMap<String, String> queryParams) {
        if (string.equals("")) {
            return null;
        }
        JsonReader reader = getReader(string, queryParams);
        JsonArray res = reader.readArray();
        reader.close();
        return res;
    }

    public JsonArray getJsonArray(String string) {
        return getJsonArray(string, null);
    }

    public List<Map<String, String>> getListMapStrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        JsonArray arr = getJsonArray(string, queryParams);
        List<Map<String, String>> res = new ArrayList<Map<String, String>>();
        for (int i = 0; i < arr.size(); i++) {
            res.add(mapStrFromJArr(arr.getJsonArray(i)));
        }
        return res;
    }

    public List<Map<String, String>> getListMapStrValue(String string) {
        return getListMapStrValue(string, null);
    }

    public TabularData getCQLResult(String string) {
        // TODO Auto-generated method stub
        return null;
    }

    public JsonObject getJsonObj(String string,
            MultivaluedMap<String, String> queryParams) {
        if (string.equals("")) {
            return null;
        }
        JsonReader reader = getReader(string, queryParams);
        JsonObject res = reader.readObject();
        reader.close();
        return res;
    }

    public HistogramValues getHistogramValue(String url,
            MultivaluedMap<String, String> queryParams) {
        HistogramValues res = new HistogramValues();
        JsonObject obj = getJsonObj(url, queryParams);
        res.count = obj.getJsonNumber("count").longValue();
        res.max = obj.getJsonNumber("max").longValue();
        res.min = obj.getJsonNumber("min").longValue();
        res.sum = obj.getJsonNumber("sum").longValue();
        res.variance = obj.getJsonNumber("variance").doubleValue();
        res.mean = obj.getJsonNumber("mean").doubleValue();
        JsonArray arr = obj.getJsonArray("sample");
        if (arr != null) {
            res.sample = new long[arr.size()];
            for (int i = 0; i < arr.size(); i++) {
                res.sample[i] = arr.getJsonNumber(i).longValue();
            }
        }
        return res;
    }

    public HistogramValues getHistogramValue(String url) {
        return getHistogramValue(url, null);
    }

    public EstimatedHistogram getEstimatedHistogram(String string,
            MultivaluedMap<String, String> queryParams, long duration) {
        String key = getCacheKey(string, queryParams, duration);
        EstimatedHistogram res = getEstimatedHistogramFromCache(key, duration);
        if (res != null) {
            return res;
        }
        res = new EstimatedHistogram(getEstimatedHistogramAsLongArrValue(string, queryParams));
        if (duration > 0) {
            cache.put(key, new CacheEntry(res));
        }
        return res;

    }
    public long[] getEstimatedHistogramAsLongArrValue(String string,
            MultivaluedMap<String, String> queryParams) {
        JsonObject obj = getJsonObj(string, queryParams);
        JsonArray arr = obj.getJsonArray("buckets");
        long res[] = new long[arr.size()];
        for (int i = 0; i< arr.size(); i++) {
            res[i] = arr.getJsonNumber(i).longValue();
        }
        return res;
    }

    public long[] getEstimatedHistogramAsLongArrValue(String string) {
        return getEstimatedHistogramAsLongArrValue(string, null);
    }
}
