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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

package com.scylladb.jmx.utils;

import java.io.*;
import java.text.DecimalFormat;

public class FileUtils
{
    private static final double KB = 1024d;
    private static final double MB = 1024*1024d;
    private static final double GB = 1024*1024*1024d;
    private static final double TB = 1024*1024*1024*1024d;

    private static final DecimalFormat df = new DecimalFormat("#.##");
 

    public static String stringifyFileSize(double value)
    {
        double d;
        if ( value >= TB )
        {
            d = value / TB;
            String val = df.format(d);
            return val + " TB";
        }
        else if ( value >= GB )
        {
            d = value / GB;
            String val = df.format(d);
            return val + " GB";
        }
        else if ( value >= MB )
        {
            d = value / MB;
            String val = df.format(d);
            return val + " MB";
        }
        else if ( value >= KB )
        {
            d = value / KB;
            String val = df.format(d);
            return val + " KB";
        }
        else
        {
            String val = df.format(value);
            return val + " bytes";
        }
    }

 
    /**
     * Get the size of a directory in bytes
     * @param directory The directory for which we need size.
     * @return The size of the directory
     */
    public static long folderSize(File directory)
    {
        long length = 0;
        for (File file : directory.listFiles())
        {
            if (file.isFile())
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
    }
 }
