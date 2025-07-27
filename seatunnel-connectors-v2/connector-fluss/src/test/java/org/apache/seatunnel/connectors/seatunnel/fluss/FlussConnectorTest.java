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

package org.apache.seatunnel.connectors.seatunnel.fluss;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fluss.util.FlussUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public static class FlussConnectorTest {

    @Test
    public void testFlussSourceConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("scan.startup.mode", "earliest");
        configMap.put("fetch.size", 1000);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussSourceConfig sourceConfig = new FlussSourceConfig(config);

        assertEquals("localhost:9092", sourceConfig.getBootstrapServers());
        assertEquals("test_db", sourceConfig.getDatabase());
        assertEquals("test_table", sourceConfig.getTable());
        assertEquals(StartupMode.EARLIEST, sourceConfig.getScanStartupMode());
        assertEquals(1000, sourceConfig.getFetchSize());
        assertEquals("test_db.test_table", sourceConfig.getFullTableName());
    }

    @Test
    public void testFlussSinkConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("batch.size", 1000);
        configMap.put("write.mode", "append");
        configMap.put("enable.transaction", true);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussSinkConfig sinkConfig = new FlussSinkConfig(config);

        assertEquals("localhost:9092", sinkConfig.getBootstrapServers());
        assertEquals("test_db", sinkConfig.getDatabase());
        assertEquals("test_table", sinkConfig.getTable());
        assertEquals(1000, sinkConfig.getBatchSize());
        assertEquals("append", sinkConfig.getWriteMode());
        assertTrue(sinkConfig.getEnableTransaction());
        assertEquals("test_db.test_table", sinkConfig.getFullTableName());
    }

    @Test
    public void testFlussUtilValidation() {
        // Test valid names
        assertDoesNotThrow(() -> FlussUtil.validateTableName("valid_table"));
        assertDoesNotThrow(() -> FlussUtil.validateDatabaseName("valid_db"));
        assertDoesNotThrow(() -> FlussUtil.validateBootstrapServers("localhost:9092"));
        assertDoesNotThrow(() -> FlussUtil.validateBootstrapServers("host1:9092,host2:9092"));

        // Test invalid names
        assertThrows(Exception.class, () -> FlussUtil.validateTableName(""));
        assertThrows(Exception.class, () -> FlussUtil.validateTableName("123invalid"));
        assertThrows(Exception.class, () -> FlussUtil.validateDatabaseName(""));
        assertThrows(Exception.class, () -> FlussUtil.validateBootstrapServers(""));
        assertThrows(Exception.class, () -> FlussUtil.validateBootstrapServers("invalid"));
    }

    @Test
    public void testStartupModeParser() {
        assertEquals(StartupMode.EARLIEST, FlussUtil.parseStartupMode("earliest"));
        assertEquals(StartupMode.LATEST, FlussUtil.parseStartupMode("latest"));
        assertEquals(StartupMode.TIMESTAMP, FlussUtil.parseStartupMode("timestamp"));
        assertEquals(StartupMode.EARLIEST, FlussUtil.parseStartupMode(null));
        assertEquals(
                StartupMode.EARLIEST,
                FlussUtil.parseStartupMode("invalid")); // Should default to EARLIEST
    }

    @Test
    public void testWriteModeParser() {
        assertEquals(FlussUtil.WriteMode.APPEND, FlussUtil.parseWriteMode("append"));
        assertEquals(FlussUtil.WriteMode.UPSERT, FlussUtil.parseWriteMode("upsert"));
        assertEquals(FlussUtil.WriteMode.APPEND, FlussUtil.parseWriteMode(null));

        assertThrows(Exception.class, () -> FlussUtil.parseWriteMode("invalid"));
    }

    @Test
    public void testFullTableName() {
        String fullName = FlussUtil.createFullTableName("test_db", "test_table");
        assertEquals("test_db.test_table", fullName);
    }

    @Test
    public void testStartupModeEnum() {
        // Test enum values
        assertEquals("earliest", StartupMode.EARLIEST.getValue());
        assertEquals("latest", StartupMode.LATEST.getValue());
        assertEquals("timestamp", StartupMode.TIMESTAMP.getValue());

        // Test fromValue method
        assertEquals(StartupMode.EARLIEST, StartupMode.fromValue("earliest"));
        assertEquals(StartupMode.LATEST, StartupMode.fromValue("latest"));
        assertEquals(StartupMode.TIMESTAMP, StartupMode.fromValue("timestamp"));
        assertEquals(StartupMode.EARLIEST, StartupMode.fromValue("EARLIEST")); // Case insensitive
        assertEquals(StartupMode.EARLIEST, StartupMode.fromValue("invalid")); // Default to EARLIEST
        assertEquals(StartupMode.EARLIEST, StartupMode.fromValue(null)); // Default to EARLIEST

        // Test toString
        assertEquals("earliest", StartupMode.EARLIEST.toString());
        assertEquals("latest", StartupMode.LATEST.toString());
        assertEquals("timestamp", StartupMode.TIMESTAMP.toString());
    }

    @Test
    public void testSourceConfigValidation() {
        // Test timestamp mode without timestamp value
        Map<String, Object> invalidConfigMap = new HashMap<>();
        invalidConfigMap.put("bootstrap.servers", "localhost:9092");
        invalidConfigMap.put("database", "test_db");
        invalidConfigMap.put("table", "test_table");
        invalidConfigMap.put("scan.startup.mode", "timestamp");
        // Missing scan.startup.timestamp

        ReadonlyConfig invalidConfig = ReadonlyConfig.fromMap(invalidConfigMap);
        assertThrows(FlussConnectorException.class, () -> new FlussSourceConfig(invalidConfig));

        // Test invalid timestamp value
        Map<String, Object> invalidTimestampMap = new HashMap<>();
        invalidTimestampMap.put("bootstrap.servers", "localhost:9092");
        invalidTimestampMap.put("database", "test_db");
        invalidTimestampMap.put("table", "test_table");
        invalidTimestampMap.put("scan.startup.mode", "timestamp");
        invalidTimestampMap.put("scan.startup.timestamp", -1L); // Invalid negative timestamp

        ReadonlyConfig invalidTimestampConfig = ReadonlyConfig.fromMap(invalidTimestampMap);
        assertThrows(
                FlussConnectorException.class, () -> new FlussSourceConfig(invalidTimestampConfig));

        // Test missing required fields
        Map<String, Object> missingFieldsMap = new HashMap<>();
        missingFieldsMap.put("bootstrap.servers", ""); // Empty bootstrap servers
        missingFieldsMap.put("database", "test_db");
        missingFieldsMap.put("table", "test_table");

        ReadonlyConfig missingFieldsConfig = ReadonlyConfig.fromMap(missingFieldsMap);
        assertThrows(
                FlussConnectorException.class, () -> new FlussSourceConfig(missingFieldsConfig));
    }

    @Test
    public void testSinkConfigValidation() {
        // Test invalid write mode
        Map<String, Object> invalidWriteModeMap = new HashMap<>();
        invalidWriteModeMap.put("bootstrap.servers", "localhost:9092");
        invalidWriteModeMap.put("database", "test_db");
        invalidWriteModeMap.put("table", "test_table");
        invalidWriteModeMap.put("write.mode", "invalid_mode");

        ReadonlyConfig invalidWriteModeConfig = ReadonlyConfig.fromMap(invalidWriteModeMap);
        assertThrows(
                FlussConnectorException.class, () -> new FlussSinkConfig(invalidWriteModeConfig));

        // Test invalid batch size
        Map<String, Object> invalidBatchSizeMap = new HashMap<>();
        invalidBatchSizeMap.put("bootstrap.servers", "localhost:9092");
        invalidBatchSizeMap.put("database", "test_db");
        invalidBatchSizeMap.put("table", "test_table");
        invalidBatchSizeMap.put("batch.size", 0); // Invalid batch size

        ReadonlyConfig invalidBatchSizeConfig = ReadonlyConfig.fromMap(invalidBatchSizeMap);
        assertThrows(
                FlussConnectorException.class, () -> new FlussSinkConfig(invalidBatchSizeConfig));

        // Test exactly-once without transaction
        Map<String, Object> exactlyOnceMap = new HashMap<>();
        exactlyOnceMap.put("bootstrap.servers", "localhost:9092");
        exactlyOnceMap.put("database", "test_db");
        exactlyOnceMap.put("table", "test_table");
        exactlyOnceMap.put("enable.exactly.once", true);
        exactlyOnceMap.put("enable.transaction", false); // Inconsistent configuration

        ReadonlyConfig exactlyOnceConfig = ReadonlyConfig.fromMap(exactlyOnceMap);
        assertThrows(FlussConnectorException.class, () -> new FlussSinkConfig(exactlyOnceConfig));
    }
}
