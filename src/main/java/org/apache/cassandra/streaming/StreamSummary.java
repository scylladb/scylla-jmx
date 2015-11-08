/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

package org.apache.cassandra.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import javax.json.JsonArray;
import javax.json.JsonObject;

import com.google.common.base.Objects;

/**
 * Summary of streaming.
 */
public class StreamSummary
{
    public final UUID cfId;

    /**
     * Number of files to transfer. Can be 0 if nothing to transfer for some streaming request.
     */
    public final int files;
    public final long totalSize;

    public StreamSummary(UUID cfId, int files, long totalSize)
    {
        this.cfId = cfId;
        this.files = files;
        this.totalSize = totalSize;
    }

    public StreamSummary(String cfId, int files, long totalSize) {
        this(UUID.fromString(cfId), files, totalSize);
    }

    public static StreamSummary fromJsonObject(JsonObject obj) {
        return new StreamSummary(obj.getString("cf_id"), obj.getInt("files"), obj.getJsonNumber("total_size").longValue());
    }

    public static Collection<StreamSummary> fromJsonArr(JsonArray arr) {
        Collection<StreamSummary> res = new ArrayList<StreamSummary>();
        for (int i = 0; i < arr.size(); i++) {
            res.add(fromJsonObject(arr.getJsonObject(i)));
        }
        return res;
    }
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamSummary summary = (StreamSummary) o;
        return files == summary.files && totalSize == summary.totalSize && cfId.equals(summary.cfId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(cfId, files, totalSize);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("StreamSummary{");
        sb.append("path=").append(cfId);
        sb.append(", files=").append(files);
        sb.append(", totalSize=").append(totalSize);
        sb.append('}');
        return sb.toString();
    }
}
