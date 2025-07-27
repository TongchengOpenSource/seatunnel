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

package org.apache.seatunnel.connectors.seatunnel.fluss.source.offset;

import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartupMode;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

/**
 * Utility class for working with {@link OffsetsInitializer} instances.
 * 
 * <p>This class provides helper methods for:
 * <ul>
 *   <li>Parsing timestamp strings in various formats</li>
 *   <li>Validating offset initialization parameters</li>
 *   <li>Converting between different timestamp representations</li>
 * </ul>
 */
@Slf4j
public final class OffsetsInitializerUtils {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    private static final Pattern NUMERIC_TIMESTAMP_PATTERN = Pattern.compile("^\\d+$");

    private OffsetsInitializerUtils() {
        // Utility class
    }

    /**
     * Parse a timestamp string that can be either:
     * <ul>
     *   <li>A numeric timestamp in milliseconds (e.g., "1678883047356")</li>
     *   <li>A formatted timestamp string (e.g., "2023-12-09 23:09:12")</li>
     * </ul>
     *
     * @param timestampStr the timestamp string to parse
     * @return the timestamp in milliseconds since epoch
     * @throws IllegalArgumentException if the timestamp string is invalid
     */
    public static long parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Timestamp string cannot be null or empty");
        }

        String trimmed = timestampStr.trim();

        // Try parsing as numeric timestamp first
        if (NUMERIC_TIMESTAMP_PATTERN.matcher(trimmed).matches()) {
            try {
                long timestamp = Long.parseLong(trimmed);
                if (timestamp <= 0) {
                    throw new IllegalArgumentException(
                            "Numeric timestamp must be positive, got: " + timestamp);
                }
                return timestamp;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid numeric timestamp: " + trimmed, e);
            }
        }

        // Try parsing as formatted timestamp
        try {
            LocalDateTime dateTime = LocalDateTime.parse(trimmed, TIMESTAMP_FORMATTER);
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "Invalid timestamp format. Expected either numeric timestamp (milliseconds) " +
                    "or 'yyyy-MM-dd HH:mm:ss' format, got: " + trimmed, e);
        }
    }

    /**
     * Format a timestamp in milliseconds to a human-readable string.
     *
     * @param timestamp the timestamp in milliseconds since epoch
     * @return a formatted timestamp string
     */
    public static String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
                .format(TIMESTAMP_FORMATTER);
    }

    /**
     * Validate that the given timestamp is valid for the specified startup mode.
     *
     * @param startupMode the startup mode
     * @param timestamp the timestamp to validate (can be null for non-timestamp modes)
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateTimestampForMode(StartupMode startupMode, @Nullable Long timestamp) {
        switch (startupMode) {
            case TIMESTAMP:
                if (timestamp == null) {
                    throw new IllegalArgumentException(
                            "Timestamp must be provided when using TIMESTAMP startup mode");
                }
                if (timestamp <= 0) {
                    throw new IllegalArgumentException(
                            "Timestamp must be positive, got: " + timestamp);
                }
                break;
            case EARLIEST:
            case LATEST:
            case FULL:
                if (timestamp != null) {
                    log.warn("Timestamp {} provided for startup mode {} will be ignored", 
                            timestamp, startupMode);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported startup mode: " + startupMode);
        }
    }

    /**
     * Create an OffsetsInitializer from a startup mode and optional timestamp string.
     *
     * @param startupMode the startup mode
     * @param timestampStr the timestamp string (required for TIMESTAMP mode, ignored for others)
     * @return the appropriate OffsetsInitializer
     * @throws IllegalArgumentException if parameters are invalid
     */
    public static OffsetsInitializer createFromModeAndTimestamp(
            StartupMode startupMode, @Nullable String timestampStr) {
        
        Long timestamp = null;
        if (timestampStr != null && !timestampStr.trim().isEmpty()) {
            timestamp = parseTimestamp(timestampStr);
        }

        validateTimestampForMode(startupMode, timestamp);
        return OffsetsInitializer.fromStartupMode(startupMode, timestamp);
    }

    /**
     * Get a human-readable description of the given OffsetsInitializer.
     *
     * @param initializer the OffsetsInitializer to describe
     * @return a description string
     */
    public static String describe(OffsetsInitializer initializer) {
        if (initializer == null) {
            return "null";
        }

        StartupMode mode = initializer.getStartupMode();
        switch (mode) {
            case EARLIEST:
                return "earliest offsets";
            case LATEST:
                return "latest offsets";
            case FULL:
                return "full snapshot then changelog";
            case TIMESTAMP:
                Long timestamp = initializer.getTimestamp();
                if (timestamp != null) {
                    return "timestamp " + timestamp + " (" + formatTimestamp(timestamp) + ")";
                } else {
                    return "timestamp (unknown)";
                }
            default:
                return "unknown mode: " + mode;
        }
    }
}
