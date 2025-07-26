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

package org.apache.seatunnel.connectors.seatunnel.fluss.util;

import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

@Slf4j
public class FlussUtil {

    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");
    private static final Pattern DATABASE_NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");

    /**
     * Validate Fluss table name
     */
    public static void validateTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Table name cannot be null or empty");
        }

        if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Invalid table name: " + tableName + 
                    ". Table name must start with a letter and contain only letters, numbers, and underscores");
        }
    }

    /**
     * Validate Fluss database name
     */
    public static void validateDatabaseName(String databaseName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Database name cannot be null or empty");
        }

        if (!DATABASE_NAME_PATTERN.matcher(databaseName).matches()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Invalid database name: " + databaseName + 
                    ". Database name must start with a letter and contain only letters, numbers, and underscores");
        }
    }

    /**
     * Validate bootstrap servers format
     */
    public static void validateBootstrapServers(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Bootstrap servers cannot be null or empty");
        }

        String[] servers = bootstrapServers.split(",");
        for (String server : servers) {
            String trimmedServer = server.trim();
            if (!trimmedServer.contains(":")) {
                throw new FlussConnectorException(
                        FlussConnectorErrorCode.INVALID_CONFIGURATION,
                        "Invalid bootstrap server format: " + trimmedServer + 
                        ". Expected format: host:port");
            }
        }
    }

    /**
     * Convert map to Properties
     */
    public static Properties mapToProperties(Map<String, String> map) {
        Properties properties = new Properties();
        if (map != null) {
            properties.putAll(map);
        }
        return properties;
    }

    /**
     * Convert map to Properties with Object values
     */
    public static Properties mapToPropertiesWithObjects(Map<String, Object> map) {
        Properties properties = new Properties();
        if (map != null) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        return properties;
    }

    /**
     * Create full table name
     */
    public static String createFullTableName(String database, String table) {
        validateDatabaseName(database);
        validateTableName(table);
        return database + "." + table;
    }

    /**
     * Parse startup mode
     */
    public static StartupMode parseStartupMode(String mode) {
        return StartupMode.fromValue(mode);
    }

    /**
     * Parse write mode
     */
    public static WriteMode parseWriteMode(String mode) {
        if (mode == null) {
            return WriteMode.APPEND;
        }

        try {
            return WriteMode.valueOf(mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Invalid write mode: " + mode + 
                    ". Valid modes are: append, upsert");
        }
    }



    /**
     * Write mode enumeration
     */
    public enum WriteMode {
        APPEND,
        UPSERT
    }

    /**
     * Retry with exponential backoff
     */
    public static void retryWithBackoff(Runnable operation, int maxRetries, long initialDelayMs) {
        Exception lastException = null;
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                operation.run();
                return; // Success
            } catch (Exception e) {
                lastException = e;
                
                if (attempt == maxRetries) {
                    break; // Last attempt failed
                }
                
                long delay = initialDelayMs * (1L << attempt); // Exponential backoff
                log.warn("Operation failed on attempt {}, retrying in {}ms", attempt + 1, delay, e);
                
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new FlussConnectorException(
                            FlussConnectorErrorCode.INTERNAL_ERROR,
                            "Interrupted during retry", ie);
                }
            }
        }
        
        throw new FlussConnectorException(
                FlussConnectorErrorCode.INTERNAL_ERROR,
                "Operation failed after " + (maxRetries + 1) + " attempts",
                lastException);
    }
}
