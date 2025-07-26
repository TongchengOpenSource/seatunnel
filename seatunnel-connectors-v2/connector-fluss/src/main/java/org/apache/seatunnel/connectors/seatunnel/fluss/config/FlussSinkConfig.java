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
public class FlussSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String bootstrapServers;
    private String database;
    private String table;
    private Integer batchSize;
    private Long batchTimeoutMs;
    private Integer maxRetries;
    private String writeMode;
    private Boolean enableTransaction;
    private String transactionTimeout;
    private Boolean enableExactlyOnce;
    private String transactionalIdPrefix;
    private Integer connectionTimeoutMs;
    private Integer requestTimeoutMs;
    private Integer retryTimes;
    private Long retryIntervalMs;
    private Map<String, String> flussConfig;

    public FlussSinkConfig(ReadonlyConfig config) {
        this.bootstrapServers = config.get(FlussSinkOptions.BOOTSTRAP_SERVERS);
        this.database = config.get(FlussSinkOptions.DATABASE);
        this.table = config.get(FlussSinkOptions.TABLE);
        this.batchSize = config.get(FlussSinkOptions.BATCH_SIZE);
        this.batchTimeoutMs = config.get(FlussSinkOptions.BATCH_TIMEOUT_MS);
        this.maxRetries = config.get(FlussSinkOptions.MAX_RETRIES);
        this.writeMode = config.get(FlussSinkOptions.WRITE_MODE);
        this.enableTransaction = config.get(FlussSinkOptions.ENABLE_TRANSACTION);
        this.transactionTimeout = config.get(FlussSinkOptions.TRANSACTION_TIMEOUT);
        this.enableExactlyOnce = config.get(FlussSinkOptions.ENABLE_EXACTLY_ONCE);
        this.transactionalIdPrefix = config.get(FlussSinkOptions.TRANSACTIONAL_ID_PREFIX);
        this.connectionTimeoutMs = config.get(FlussSinkOptions.CONNECTION_TIMEOUT_MS);
        this.requestTimeoutMs = config.get(FlussSinkOptions.REQUEST_TIMEOUT_MS);
        this.retryTimes = config.get(FlussSinkOptions.RETRY_TIMES);
        this.retryIntervalMs = config.get(FlussSinkOptions.RETRY_INTERVAL_MS);
        this.flussConfig = config.getOptional(FlussSinkOptions.FLUSS_CONFIG).orElse(new HashMap<>());

        // Validate configuration
        validateConfiguration();
    }

    /**
     * Validate the configuration for consistency and completeness
     */
    private void validateConfiguration() {
        // Validate required configurations
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

        // Validate write mode
        if (writeMode != null && !writeMode.equals("append") && !writeMode.equals("upsert")) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Invalid write mode: " + writeMode + ". Valid modes are: 'append', 'upsert'.");
        }

        // Validate batch size
        if (batchSize != null && batchSize <= 0) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Batch size must be greater than 0, but got: " + batchSize);
        }

        // Validate batch timeout
        if (batchTimeoutMs != null && batchTimeoutMs <= 0) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Batch timeout must be greater than 0, but got: " + batchTimeoutMs + "ms");
        }

        // Validate exactly-once configuration
        if (enableExactlyOnce != null && enableExactlyOnce && (enableTransaction == null || !enableTransaction)) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Exactly-once semantics requires transactions to be enabled. " +
                    "Please set 'enable.transaction' to true when using 'enable.exactly.once'.");
        }
    }

    public Map<String, Object> toFlussProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("connection.timeout.ms", connectionTimeoutMs);
        properties.put("request.timeout.ms", requestTimeoutMs);
        properties.put("retry.times", retryTimes);
        properties.put("retry.interval.ms", retryIntervalMs);
        properties.put("batch.size", batchSize);
        properties.put("batch.timeout.ms", batchTimeoutMs);
        properties.put("max.retries", maxRetries);
        
        if (enableTransaction) {
            properties.put("enable.transaction", true);
            properties.put("transaction.timeout", transactionTimeout);
        }
        
        if (enableExactlyOnce) {
            properties.put("enable.exactly.once", true);
            properties.put("transactional.id.prefix", transactionalIdPrefix);
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
