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

package org.apache.seatunnel.connectors.seatunnel.fluss.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussOptions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

static
/**
 * Unit tests for {@link FlussCatalog}.
 *
 * <p>Note: These tests are designed to work without an actual Fluss cluster. They test the catalog
 * configuration and basic functionality.
 */
class FlussCatalogTest {

    @Test
    void testCatalogCreation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("default-database", "test_db");
        configMap.put("case-sensitive", false);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussCatalog catalog = new FlussCatalog("test_catalog", config);

        assertEquals("test_catalog", catalog.name());
        assertEquals("test_db", catalog.getDefaultDatabase());
    }

    @Test
    void testCatalogCreationWithDefaults() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussCatalog catalog = new FlussCatalog("test_catalog", config);

        assertEquals("test_catalog", catalog.name());
        assertEquals("fluss", catalog.getDefaultDatabase()); // Default value
    }

    @Test
    void testCatalogOpenWithInvalidConfig() {
        Map<String, Object> configMap = new HashMap<>();
        // Missing required bootstrap.servers

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussCatalog catalog = new FlussCatalog("test_catalog", config);

        assertThrows(CatalogException.class, catalog::open);
    }

    @Test
    void testCatalogConfiguration() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("default-database", "custom_db");
        configMap.put("case-sensitive", true);
        configMap.put("metadata.cache.ttl.ms", 600000L);
        configMap.put("metadata.cache.size", 2000);
        configMap.put("connection.pool.size", 20);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussCatalog catalog = new FlussCatalog("test_catalog", config);

        assertEquals("test_catalog", catalog.name());
        assertEquals("custom_db", catalog.getDefaultDatabase());
    }

    @Test
    void testCatalogWithFlussConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("default-database", "test_db");

        Map<String, String> flussConfig = new HashMap<>();
        flussConfig.put("client.id", "test-catalog-client");
        flussConfig.put("connection.timeout.ms", "30000");
        flussConfig.put("request.timeout.ms", "30000");
        configMap.put("fluss.config", flussConfig);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussCatalog catalog = new FlussCatalog("test_catalog", config);

        assertEquals("test_catalog", catalog.name());
        assertEquals("test_db", catalog.getDefaultDatabase());
    }

    @Test
    void testCatalogClose() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussCatalog catalog = new FlussCatalog("test_catalog", config);

        // Should not throw exception even if not opened
        assertDoesNotThrow(catalog::close);
    }

    @Test
    void testCatalogFactoryCreation() {
        FlussCatalogFactory factory = new FlussCatalogFactory();

        assertEquals("Fluss", factory.factoryIdentifier());
        assertNotNull(factory.optionRule());

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FlussCatalog catalog = (FlussCatalog) factory.createCatalog("test_catalog", config);

        assertNotNull(catalog);
        assertEquals("test_catalog", catalog.name());
    }

    @Test
    void testCatalogFactoryOptionRule() {
        FlussCatalogFactory factory = new FlussCatalogFactory();

        // Test that the option rule includes required options
        assertNotNull(factory.optionRule());

        // The option rule should require bootstrap.servers
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should not throw exception with required options
        assertDoesNotThrow(() -> factory.createCatalog("test", config));
    }

    @Test
    void testCatalogOptionsDefaults() {
        // Test default values for catalog options
        assertEquals("fluss", FlussCatalogOptions.DEFAULT_DATABASE.defaultValue());
    }

    @Test
    void testCatalogOptionsKeys() {
        // Test option keys
        assertEquals("default-database", FlussCatalogOptions.DEFAULT_DATABASE.key());
    }

    @Test
    void testCatalogSimplified() {
        // Test that FlussCatalogOptions no longer extends FlussOptions
        assertFalse(FlussCatalogOptions.class.getSuperclass().equals(FlussOptions.class));

        // Test that we can still access base options
        assertNotNull(FlussOptions.BOOTSTRAP_SERVERS);
        assertNotNull(FlussOptions.DATABASE);
        assertNotNull(FlussOptions.TABLE);
    }
}
