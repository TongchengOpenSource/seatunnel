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

import java.util.HashMap;
import java.util.Map;

static
/** Unit tests for {@link FlussSinkConfig}. */
class FlussSinkConfigTest {

    @Test
    void testMinimalConfiguration() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussSinkConfig sinkConfig = new FlussSinkConfig(config);

        assertEquals("localhost:9092", sinkConfig.getBootstrapServers());
        assertEquals("test_db", sinkConfig.getDatabase());
        assertEquals("test_table", sinkConfig.getTable());
        assertEquals("test_db.test_table", sinkConfig.getFullTableName());
        assertNotNull(sinkConfig.getFlussProperties());
    }

    @Test
    void testConfigurationWithBucketOptions() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("bucket.num", 16);
        configMap.put("bucket.key", "user_id");
        configMap.put("sink.ignore-delete", true);
        configMap.put("sink.bucket-shuffle", false);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussSinkConfig sinkConfig = new FlussSinkConfig(config);

        Map<String, Object> properties = sinkConfig.getFlussProperties();
        assertEquals("localhost:9092", properties.get("bootstrap.servers"));
        assertEquals(16, properties.get("bucket.num"));
        assertEquals("user_id", properties.get("bucket.key"));
        assertEquals(true, properties.get("sink.ignore-delete"));
        assertEquals(false, properties.get("sink.bucket-shuffle"));
    }

    @Test
    void testConfigurationWithFlussConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");

        Map<String, String> flussConfig = new HashMap<>();
        flussConfig.put("batch.size", "2000");
        flussConfig.put("batch.timeout.ms", "10000");
        flussConfig.put("enable.transaction", "true");
        flussConfig.put("client.id", "test-sink");
        configMap.put("fluss.config", flussConfig);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussSinkConfig sinkConfig = new FlussSinkConfig(config);

        Map<String, Object> properties = sinkConfig.getFlussProperties();
        assertEquals("localhost:9092", properties.get("bootstrap.servers"));
        assertEquals("2000", properties.get("batch.size"));
        assertEquals("10000", properties.get("batch.timeout.ms"));
        assertEquals("true", properties.get("enable.transaction"));
        assertEquals("test-sink", properties.get("client.id"));
    }

    @Test
    void testConfigurationValidationMissingBootstrapServers() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        // Missing bootstrap.servers

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertThrows(FlussConnectorException.class, () -> new FlussSinkConfig(config));
    }

    @Test
    void testConfigurationValidationMissingDatabase() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("table", "test_table");
        // Missing database

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertThrows(FlussConnectorException.class, () -> new FlussSinkConfig(config));
    }

    @Test
    void testConfigurationValidationMissingTable() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        // Missing table

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertThrows(FlussConnectorException.class, () -> new FlussSinkConfig(config));
    }

    @Test
    void testConfigurationValidationEmptyValues() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "");
        configMap.put("database", "   ");
        configMap.put("table", "test_table");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertThrows(FlussConnectorException.class, () -> new FlussSinkConfig(config));
    }

    @Test
    void testCompleteConfiguration() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "host1:9092,host2:9092");
        configMap.put("database", "production");
        configMap.put("table", "orders");
        configMap.put("bucket.num", 32);
        configMap.put("bucket.key", "customer_id,region");
        configMap.put("sink.ignore-delete", false);
        configMap.put("sink.bucket-shuffle", true);
        configMap.put("lookup.async", true);

        Map<String, String> flussConfig = new HashMap<>();
        flussConfig.put("batch.size", "5000");
        flussConfig.put("batch.timeout.ms", "8000");
        flussConfig.put("enable.transaction", "true");
        flussConfig.put("transaction.timeout", "15min");
        flussConfig.put("enable.exactly.once", "false");
        flussConfig.put("transactional.id.prefix", "production-orders");
        flussConfig.put("client.id", "production-sink");
        flussConfig.put("connection.timeout.ms", "30000");
        flussConfig.put("compression.type", "lz4");
        configMap.put("fluss.config", flussConfig);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussSinkConfig sinkConfig = new FlussSinkConfig(config);

        assertEquals("host1:9092,host2:9092", sinkConfig.getBootstrapServers());
        assertEquals("production", sinkConfig.getDatabase());
        assertEquals("orders", sinkConfig.getTable());
        assertEquals("production.orders", sinkConfig.getFullTableName());

        Map<String, Object> properties = sinkConfig.getFlussProperties();
        assertEquals("host1:9092,host2:9092", properties.get("bootstrap.servers"));
        assertEquals(32, properties.get("bucket.num"));
        assertEquals("customer_id,region", properties.get("bucket.key"));
        assertEquals(false, properties.get("sink.ignore-delete"));
        assertEquals(true, properties.get("sink.bucket-shuffle"));
        assertEquals(true, properties.get("lookup.async"));

        // Verify fluss.config properties
        assertEquals("5000", properties.get("batch.size"));
        assertEquals("8000", properties.get("batch.timeout.ms"));
        assertEquals("true", properties.get("enable.transaction"));
        assertEquals("15min", properties.get("transaction.timeout"));
        assertEquals("false", properties.get("enable.exactly.once"));
        assertEquals("production-orders", properties.get("transactional.id.prefix"));
        assertEquals("production-sink", properties.get("client.id"));
        assertEquals("30000", properties.get("connection.timeout.ms"));
        assertEquals("lz4", properties.get("compression.type"));
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussSinkConfig sinkConfig = new FlussSinkConfig(config);

        Map<String, Object> properties = sinkConfig.getFlussProperties();

        // Verify default values from FlussOptions
        assertEquals(false, properties.get("sink.ignore-delete"));
        assertEquals(true, properties.get("sink.bucket-shuffle"));
        assertEquals(true, properties.get("lookup.async"));
    }
}
