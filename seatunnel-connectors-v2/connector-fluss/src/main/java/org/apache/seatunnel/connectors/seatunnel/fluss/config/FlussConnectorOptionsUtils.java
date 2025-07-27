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
import org.apache.seatunnel.connectors.seatunnel.fluss.source.offset.OffsetsInitializer;
import org.apache.seatunnel.connectors.seatunnel.fluss.source.offset.OffsetsInitializerUtils;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for Fluss connector options, inspired by the official Fluss implementation.
 * 
 * <p>This class provides helper methods for:
 * <ul>
 *   <li>Validating configuration options</li>
 *   <li>Converting between different configuration formats</li>
 *   <li>Creating startup options from configuration</li>
 *   <li>Building Fluss client properties</li>
 * </ul>
 */
@Slf4j
public class FlussConnectorOptionsUtils {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private FlussConnectorOptionsUtils() {
        // Utility class
    }

    /**
     * Validate source configuration options.
     *
     * @param config the configuration to validate
     * @throws FlussConnectorException if validation fails
     */
    public static void validateSourceOptions(ReadonlyConfig config) {
        validateScanStartupMode(config);
        validateRequiredOptions(config);
    }

    /**
     * Validate sink configuration options.
     *
     * @param config the configuration to validate
     * @throws FlussConnectorException if validation fails
     */
    public static void validateSinkOptions(ReadonlyConfig config) {
        validateRequiredOptions(config);
    }

    /**
     * Get startup options from configuration.
     *
     * @param config the configuration
     * @param timeZone the time zone for timestamp parsing
     * @return startup options
     */
    public static StartupOptions getStartupOptions(ReadonlyConfig config, ZoneId timeZone) {
        StartupMode startupMode = config.get(FlussSourceOptions.SCAN_STARTUP_MODE);
        String timestampStr = config.getOptional(FlussSourceOptions.SCAN_STARTUP_TIMESTAMP).orElse(null);
        
        StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        
        if (startupMode == StartupMode.TIMESTAMP) {
            if (timestampStr == null) {
                throw new FlussConnectorException(
                        FlussConnectorErrorCode.INVALID_CONFIGURATION,
                        "scan.startup.timestamp is required when using TIMESTAMP startup mode");
            }
            options.startupTimestampMs = parseTimestamp(timestampStr, timeZone);
        }
        
        // Create OffsetsInitializer
        try {
            options.offsetsInitializer = OffsetsInitializer.fromStartupMode(startupMode, options.startupTimestampMs);
        } catch (IllegalArgumentException e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "Invalid startup configuration: " + e.getMessage(), e);
        }
        
        return options;
    }

    /**
     * Get bucket keys from configuration.
     *
     * @param config the configuration
     * @return list of bucket key names
     */
    public static List<String> getBucketKeys(ReadonlyConfig config) {
        return config.getOptional(FlussOptions.BUCKET_KEY)
                .map(bucketKey -> Arrays.stream(bucketKey.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()))
                .orElse(new ArrayList<>());
    }

    /**
     * Build Fluss client properties from SeaTunnel configuration.
     *
     * @param config the SeaTunnel configuration
     * @return Fluss client properties
     */
    public static Map<String, Object> buildFlussProperties(ReadonlyConfig config) {
        Map<String, Object> properties = new HashMap<>();
        
        // Core connection properties
        properties.put("bootstrap.servers", config.get(FlussOptions.BOOTSTRAP_SERVERS));
        
        // Optional bucket configuration
        config.getOptional(FlussOptions.BUCKET_NUMBER)
                .ifPresent(bucketNum -> properties.put("bucket.num", bucketNum));
        config.getOptional(FlussOptions.BUCKET_KEY)
                .ifPresent(bucketKey -> properties.put("bucket.key", bucketKey));
        
        // Scan configuration
        config.getOptional(FlussOptions.SCAN_PARTITION_DISCOVERY_INTERVAL)
                .ifPresent(interval -> properties.put("scan.partition.discovery.interval", interval.toString()));
        
        // Lookup configuration
        properties.put("lookup.async", config.get(FlussOptions.LOOKUP_ASYNC));
        
        // Sink configuration
        properties.put("sink.ignore-delete", config.get(FlussOptions.SINK_IGNORE_DELETE));
        properties.put("sink.bucket-shuffle", config.get(FlussOptions.SINK_BUCKET_SHUFFLE));
        
        // Additional Fluss configuration
        config.getOptional(FlussOptions.FLUSS_CONFIG)
                .ifPresent(flussConfig -> {
                    log.debug("Adding additional Fluss configuration: {}", flussConfig);
                    properties.putAll(flussConfig);
                });
        
        return properties;
    }

    /**
     * Parse timestamp string to milliseconds.
     *
     * @param timestampStr the timestamp string
     * @param timeZone the time zone
     * @return timestamp in milliseconds
     */
    public static long parseTimestamp(String timestampStr, ZoneId timeZone) {
        if (timestampStr.matches("\\d+")) {
            return Long.parseLong(timestampStr);
        }
        
        try {
            return LocalDateTime.parse(timestampStr, DATE_TIME_FORMATTER)
                    .atZone(timeZone)
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    String.format(
                            "Invalid timestamp format '%s'. Expected format: 'yyyy-MM-dd HH:mm:ss' or timestamp in milliseconds. "
                                    + "Examples: '2023-12-09 23:09:12' or '1678883047356'.",
                            timestampStr), e);
        }
    }

    /**
     * Get the local time zone, defaulting to system default.
     *
     * @param timeZone the time zone string, can be null
     * @return the ZoneId
     */
    public static ZoneId getLocalTimeZone(String timeZone) {
        return timeZone == null ? ZoneId.systemDefault() : ZoneId.of(timeZone);
    }

    // ----------------------------------------------------------------------------------------
    // Private validation methods
    // ----------------------------------------------------------------------------------------

    private static void validateScanStartupMode(ReadonlyConfig config) {
        StartupMode startupMode = config.get(FlussSourceOptions.SCAN_STARTUP_MODE);
        
        if (startupMode == StartupMode.TIMESTAMP) {
            if (!config.getOptional(FlussSourceOptions.SCAN_STARTUP_TIMESTAMP).isPresent()) {
                throw new FlussConnectorException(
                        FlussConnectorErrorCode.INVALID_CONFIGURATION,
                        String.format(
                                "'%s' is required when using '%s' startup mode",
                                FlussSourceOptions.SCAN_STARTUP_TIMESTAMP.key(),
                                StartupMode.TIMESTAMP));
            }
        }
    }

    private static void validateRequiredOptions(ReadonlyConfig config) {
        // Validate bootstrap servers
        String bootstrapServers = config.get(FlussOptions.BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "bootstrap.servers cannot be null or empty");
        }
        
        // Validate database
        String database = config.get(FlussOptions.DATABASE);
        if (database == null || database.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "database cannot be null or empty");
        }
        
        // Validate table
        String table = config.get(FlussOptions.TABLE);
        if (table == null || table.trim().isEmpty()) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INVALID_CONFIGURATION,
                    "table cannot be null or empty");
        }
    }

    /**
     * Startup options container.
     */
    public static class StartupOptions {
        public StartupMode startupMode;
        public long startupTimestampMs;
        public OffsetsInitializer offsetsInitializer;
        
        @Override
        public String toString() {
            return "StartupOptions{" +
                    "startupMode=" + startupMode +
                    ", startupTimestampMs=" + startupTimestampMs +
                    ", offsetsInitializer=" + OffsetsInitializerUtils.describe(offsetsInitializer) +
                    '}';
        }
    }
}
