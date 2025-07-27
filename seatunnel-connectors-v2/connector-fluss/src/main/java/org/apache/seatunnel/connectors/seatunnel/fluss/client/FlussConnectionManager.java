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

package org.apache.seatunnel.connectors.seatunnel.fluss.client;

import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Fluss connections and provides thread-safe access to Fluss resources. This class handles
 * connection pooling and resource cleanup.
 */
@Slf4j
public class FlussConnectionManager implements Closeable {

    private final Configuration flussConfig;
    private final Connection connection;
    private final Map<String, Table> tableCache;
    private volatile boolean closed = false;

    public FlussConnectionManager(Map<String, Object> configMap) {
        this.flussConfig = createFlussConfiguration(configMap);
        this.tableCache = new ConcurrentHashMap<>();

        try {
            this.connection = ConnectionFactory.createConnection(flussConfig);
            log.info(
                    "Successfully created Fluss connection to {}",
                    flussConfig.getString("bootstrap.servers", "unknown"));
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to create Fluss connection",
                    e);
        }
    }

    public Admin getAdmin() {
        checkNotClosed();
        try {
            return connection.getAdmin();
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.CONNECTION_FAILED, "Failed to get Admin instance", e);
        }
    }

    /** Get Table instance for data operations */
    public Table getTable(String database, String tableName) {
        checkNotClosed();

        String tableKey = database + "." + tableName;
        return tableCache.computeIfAbsent(
                tableKey,
                key -> {
                    try {
                        TablePath tablePath = TablePath.of(database, tableName);
                        Table table = connection.getTable(tablePath);
                        log.debug("Created table instance for {}", tableKey);
                        return table;
                    } catch (Exception e) {
                        throw new FlussConnectorException(
                                FlussConnectorErrorCode.TABLE_NOT_FOUND,
                                "Failed to get table: " + tableKey,
                                e);
                    }
                });
    }

    /** Check if the connection is still valid */
    public boolean isConnected() {
        return !closed && connection != null;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            tableCache.clear();
            if (connection != null) {
                connection.close();
            }
            log.info("Fluss connection closed successfully");
        } catch (Exception e) {
            log.warn("Error closing Fluss connection", e);
            throw new IOException("Failed to close Fluss connection", e);
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.CONNECTION_FAILED, "Connection is closed");
        }
    }

    private Configuration createFlussConfiguration(Map<String, Object> configMap) {
        Configuration config = new Configuration();

        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                config.setString(key, (String) value);
            } else if (value instanceof Integer) {
                config.setInteger(key, (Integer) value);
            } else if (value instanceof Long) {
                config.setLong(key, (Long) value);
            } else if (value instanceof Boolean) {
                config.setBoolean(key, (Boolean) value);
            } else if (value instanceof Double) {
                config.setDouble(key, (Double) value);
            } else if (value != null) {
                config.setString(key, value.toString());
            }
        }

        return config;
    }
}
