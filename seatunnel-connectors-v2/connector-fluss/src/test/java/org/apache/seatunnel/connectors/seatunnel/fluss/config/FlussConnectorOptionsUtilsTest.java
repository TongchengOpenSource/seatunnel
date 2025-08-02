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
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

static
/** Unit tests for {@link FlussConnectorOptionsUtils}. */
class FlussConnectorOptionsUtilsTest {

    @Test
    void testValidateSourceOptionsSuccess() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("scan.startup.mode", "earliest");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should not throw exception
        assertDoesNotThrow(() -> FlussConnectorOptionsUtils.validateSourceOptions(config));
    }

    @Test
    void testValidateSourceOptionsTimestampMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("scan.startup.mode", "timestamp");
        configMap.put("scan.startup.timestamp", "2023-03-15 10:30:47");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should not throw exception
        assertDoesNotThrow(() -> FlussConnectorOptionsUtils.validateSourceOptions(config));
    }

    @Test
    void testValidateSourceOptionsTimestampModeWithoutTimestamp() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("scan.startup.mode", "timestamp");
        // Missing scan.startup.timestamp

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertThrows(
                FlussConnectorException.class,
                () -> FlussConnectorOptionsUtils.validateSourceOptions(config));
    }

    @Test
    void testValidateSourceOptionsMissingRequired() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        // Missing database and table

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertThrows(
                FlussConnectorException.class,
                () -> FlussConnectorOptionsUtils.validateSourceOptions(config));
    }

    @Test
    void testGetStartupOptionsEarliest() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("scan.startup.mode", "earliest");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        ZoneId timeZone = ZoneId.systemDefault();

        FlussConnectorOptionsUtils.StartupOptions options =
                FlussConnectorOptionsUtils.getStartupOptions(config, timeZone);

        assertEquals(StartupMode.EARLIEST, options.startupMode);
        assertNotNull(options.offsetsInitializer);
        assertEquals(StartupMode.EARLIEST, options.offsetsInitializer.getStartupMode());
    }

    @Test
    void testGetStartupOptionsTimestamp() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("scan.startup.mode", "timestamp");
        configMap.put("scan.startup.timestamp", "1678883047356");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        ZoneId timeZone = ZoneId.systemDefault();

        FlussConnectorOptionsUtils.StartupOptions options =
                FlussConnectorOptionsUtils.getStartupOptions(config, timeZone);

        assertEquals(StartupMode.TIMESTAMP, options.startupMode);
        assertEquals(1678883047356L, options.startupTimestampMs);
        assertNotNull(options.offsetsInitializer);
        assertEquals(StartupMode.TIMESTAMP, options.offsetsInitializer.getStartupMode());
        assertEquals(1678883047356L, options.offsetsInitializer.getTimestamp());
    }

    @Test
    void testGetBucketKeys() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bucket.key", "user_id,region,category");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<String> bucketKeys = FlussConnectorOptionsUtils.getBucketKeys(config);

        assertEquals(Arrays.asList("user_id", "region", "category"), bucketKeys);
    }

    @Test
    void testGetBucketKeysEmpty() {
        Map<String, Object> configMap = new HashMap<>();
        // No bucket.key configured

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<String> bucketKeys = FlussConnectorOptionsUtils.getBucketKeys(config);

        assertTrue(bucketKeys.isEmpty());
    }

    @Test
    void testBuildFlussProperties() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("bucket.num", 16);
        configMap.put("bucket.key", "user_id");
        configMap.put("lookup.async", true);
        configMap.put("sink.ignore-delete", false);
        configMap.put("sink.bucket-shuffle", true);

        Map<String, String> flussConfig = new HashMap<>();
        flussConfig.put("client.id", "test-client");
        flussConfig.put("connection.timeout.ms", "30000");
        configMap.put("fluss.config", flussConfig);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Map<String, Object> properties = FlussConnectorOptionsUtils.buildFlussProperties(config);

        assertEquals("localhost:9092", properties.get("bootstrap.servers"));
        assertEquals(16, properties.get("bucket.num"));
        assertEquals("user_id", properties.get("bucket.key"));
        assertEquals(true, properties.get("lookup.async"));
        assertEquals(false, properties.get("sink.ignore-delete"));
        assertEquals(true, properties.get("sink.bucket-shuffle"));
        assertEquals("test-client", properties.get("client.id"));
        assertEquals("30000", properties.get("connection.timeout.ms"));
    }

    @Test
    void testParseTimestampNumeric() {
        ZoneId timeZone = ZoneId.systemDefault();
        long timestamp = FlussConnectorOptionsUtils.parseTimestamp("1678883047356", timeZone);
        assertEquals(1678883047356L, timestamp);
    }

    @Test
    void testParseTimestampFormatted() {
        ZoneId timeZone = ZoneId.systemDefault();
        long timestamp = FlussConnectorOptionsUtils.parseTimestamp("2023-03-15 10:30:47", timeZone);
        assertTrue(timestamp > 0);
    }

    @Test
    void testParseTimestampInvalid() {
        ZoneId timeZone = ZoneId.systemDefault();

        assertThrows(
                FlussConnectorException.class,
                () -> FlussConnectorOptionsUtils.parseTimestamp("invalid-timestamp", timeZone));

        assertThrows(
                FlussConnectorException.class,
                () -> FlussConnectorOptionsUtils.parseTimestamp("2023-13-45 25:70:80", timeZone));
    }

    @Test
    void testGetLocalTimeZone() {
        ZoneId defaultZone = FlussConnectorOptionsUtils.getLocalTimeZone(null);
        assertEquals(ZoneId.systemDefault(), defaultZone);

        ZoneId utcZone = FlussConnectorOptionsUtils.getLocalTimeZone("UTC");
        assertEquals(ZoneId.of("UTC"), utcZone);
    }
}
