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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class FlussSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String bootstrapServers;
    private String database;
    private String table;
    private StartupMode scanStartupMode;
    private Long scanStartupTimestamp;
    private Integer scanParallelism;
    private Integer fetchSize;
    private Long pollTimeoutMs;
    private Boolean enableChangelog;
    private String consumerGroup;
    private Integer connectionTimeoutMs;
    private Integer requestTimeoutMs;
    private Integer retryTimes;
    private Long retryIntervalMs;
    private Map<String, String> flussConfig;

    public FlussSourceConfig(ReadonlyConfig config) {
        this.bootstrapServers = config.get(FlussSourceOptions.BOOTSTRAP_SERVERS);
        this.database = config.get(FlussSourceOptions.DATABASE);
        this.table = config.get(FlussSourceOptions.TABLE);
        this.scanStartupMode = StartupMode.fromValue(config.get(FlussSourceOptions.SCAN_STARTUP_MODE));
        this.scanStartupTimestamp = config.getOptional(FlussSourceOptions.SCAN_STARTUP_TIMESTAMP).orElse(null);
        this.scanParallelism = config.get(FlussSourceOptions.SCAN_PARALLELISM);
        this.fetchSize = config.get(FlussSourceOptions.FETCH_SIZE);
        this.pollTimeoutMs = config.get(FlussSourceOptions.POLL_TIMEOUT_MS);
        this.enableChangelog = config.get(FlussSourceOptions.ENABLE_CHANGELOG);
        this.consumerGroup = config.getOptional(FlussSourceOptions.CONSUMER_GROUP).orElse(null);
        this.connectionTimeoutMs = config.get(FlussSourceOptions.CONNECTION_TIMEOUT_MS);
        this.requestTimeoutMs = config.get(FlussSourceOptions.REQUEST_TIMEOUT_MS);
        this.retryTimes = config.get(FlussSourceOptions.RETRY_TIMES);
        this.retryIntervalMs = config.get(FlussSourceOptions.RETRY_INTERVAL_MS);
        this.flussConfig = config.getOptional(FlussSourceOptions.FLUSS_CONFIG).orElse(new HashMap<>());

        // Validate configuration
        validateConfiguration();
    }

    /**
     * Validate the configuration for consistency and completeness
     */
    private void validateConfiguration() {
        // Validate timestamp mode configuration
        if (scanStartupMode == StartupMode.TIMESTAMP && scanStartupTimestamp == null) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "When using 'timestamp' startup mode, 'scan.startup.timestamp' must be provided. " +
                    "Please specify a valid timestamp value in milliseconds.");
        }

        // Validate timestamp value if provided
        if (scanStartupTimestamp != null && scanStartupTimestamp <= 0) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Invalid timestamp value: " + scanStartupTimestamp +
                    ". Timestamp must be a positive number representing milliseconds since epoch.");
        }

        // Validate other critical configurations
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Bootstrap servers cannot be null or empty. Please provide valid Fluss server addresses.");
        }

        if (database == null || database.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Database name cannot be null or empty. Please provide a valid database name.");
        }

        if (table == null || table.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Table name cannot be null or empty. Please provide a valid table name.");
        }
    }

    public Map<String, Object> toFlussProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("connection.timeout.ms", connectionTimeoutMs);
        properties.put("request.timeout.ms", requestTimeoutMs);
        properties.put("retry.times", retryTimes);
        properties.put("retry.interval.ms", retryIntervalMs);
        
        if (consumerGroup != null) {
            properties.put("group.id", consumerGroup);
        }
        
        // Add custom Fluss configuration
        if (flussConfig != null) {
            properties.putAll(flussConfig);
        }
        
        return properties;
    }

    public String getFullTableName() {
        return database + "." + table;
    }
}
