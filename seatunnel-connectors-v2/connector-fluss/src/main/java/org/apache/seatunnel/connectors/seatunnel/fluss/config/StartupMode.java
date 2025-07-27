/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.fluss.config;

/** Startup mode for Fluss source connector */
public enum StartupMode {

    /** Start reading from the earliest available offset in the log */
    EARLIEST("earliest"),

    /** Start reading from the latest available offset in the log */
    LATEST("latest"),

    /** Start reading from a specific timestamp */
    TIMESTAMP("timestamp"),

    /**
     * Perform a full snapshot on the table upon first startup, and continue to read the changelog.
     * For log tables: equivalent to EARLIEST, reading from the earliest offset.
     * For primary key tables: reads the latest snapshot which materializes all changes on the table.
     */
    FULL("full");

    private final String value;

    StartupMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /** Parse startup mode from string value */
    public static StartupMode fromValue(String value) {
        if (value == null) {
            return EARLIEST;
        }
        for (StartupMode mode : values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown start mode value: " + value);
    }

    @Override
    public String toString() {
        return value;
    }
}
