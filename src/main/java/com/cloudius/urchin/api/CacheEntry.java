/*
 * Copyright 2015 Cloudius Systems
 */

package com.cloudius.urchin.api;

public class CacheEntry {
    long time;
    public String value;

    CacheEntry(String value) {
        time = System.currentTimeMillis();
        this.value = value;
    }

    public boolean valid(long duration) {
        return (System.currentTimeMillis() - time) < duration;
    }

}
