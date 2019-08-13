package com.scylladb.jmx.utils;

import java.time.Instant;
import java.util.Date;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class DateXmlAdapter extends XmlAdapter<String, Date> {
    @Override
    public String marshal(Date v) throws Exception {
        return Instant.ofEpochMilli(v.getTime()).toString();
    }

    @Override
    public Date unmarshal(String v) throws Exception {
        return new Date(Instant.parse(v).toEpochMilli());
    }

}