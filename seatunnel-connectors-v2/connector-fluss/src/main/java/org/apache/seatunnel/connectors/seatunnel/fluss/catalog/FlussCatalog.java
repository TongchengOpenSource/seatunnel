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
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.fluss.client.FlussConnectionManager;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussConnectorOptionsUtils;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fluss.utils.FlussTypeConverter;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fluss catalog implementation for SeaTunnel.
 * 
 * <p>This catalog provides metadata operations for Fluss tables, including:
 * <ul>
 *   <li>Database operations: list, create, drop, check existence</li>
 *   <li>Table operations: list, create, drop, get metadata, check existence</li>
 *   <li>Metadata caching for improved performance</li>
 * </ul>
 * 
 * <p>The catalog follows the SeaTunnel catalog interface and integrates with
 * Fluss admin client for metadata operations.
 */
@Slf4j
public class FlussCatalog implements Catalog {

    private final String catalogName;
    private final ReadonlyConfig config;
    private final String defaultDatabase;
    private final boolean caseSensitive;
    private final Map<String, Object> flussProperties;
    
    // Connection and client management
    private FlussConnectionManager connectionManager;
    
    // Metadata cache
    private final Map<String, List<String>> databaseTablesCache;
    private final Map<TablePath, CatalogTable> tableMetadataCache;
    private final long metadataCacheTtl;
    private final int metadataCacheSize;
    
    // Cache timestamps
    private final Map<String, Long> databaseCacheTimestamps;
    private final Map<TablePath, Long> tableCacheTimestamps;

    public FlussCatalog(String catalogName, ReadonlyConfig config) {
        this.catalogName = catalogName;
        this.config = config;
        this.defaultDatabase = config.get(FlussCatalogOptions.DEFAULT_DATABASE);
        this.caseSensitive = config.get(FlussCatalogOptions.CASE_SENSITIVE);
        this.metadataCacheTtl = config.get(FlussCatalogOptions.METADATA_CACHE_TTL_MS);
        this.metadataCacheSize = config.get(FlussCatalogOptions.METADATA_CACHE_SIZE);
        
        // Build Fluss properties
        this.flussProperties = FlussConnectorOptionsUtils.buildFlussProperties(config);
        
        // Initialize caches
        this.databaseTablesCache = new ConcurrentHashMap<>();
        this.tableMetadataCache = new ConcurrentHashMap<>();
        this.databaseCacheTimestamps = new ConcurrentHashMap<>();
        this.tableCacheTimestamps = new ConcurrentHashMap<>();
        
        log.info("Created Fluss catalog '{}' with default database '{}'", catalogName, defaultDatabase);
    }

    @Override
    public void open() throws CatalogException {
        try {
            // Validate configuration
            FlussConnectorOptionsUtils.validateSinkOptions(config);

            // Initialize connection manager
            this.connectionManager = new FlussConnectionManager(flussProperties);

            // Test connection
            if (!connectionManager.isConnected()) {
                throw new CatalogException("Failed to establish connection to Fluss cluster");
            }

            log.info("Opened Fluss catalog '{}' successfully", catalogName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to open Fluss catalog '%s'", catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        try {
            if (connectionManager != null) {
                connectionManager.close();
            }
            
            // Clear caches
            databaseTablesCache.clear();
            tableMetadataCache.clear();
            databaseCacheTimestamps.clear();
            tableCacheTimestamps.clear();
            
            log.info("Closed Fluss catalog '{}' successfully", catalogName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to close Fluss catalog '%s'", catalogName), e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            String normalizedName = normalizeName(databaseName);
            Admin admin = connectionManager.getAdmin();

            // Get list of databases and check if the specified database exists
            List<String> databases = admin.listDatabases().get();
            return databases.contains(normalizedName);

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to check if database '%s' exists", databaseName), e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            // Check cache first
            String cacheKey = "databases";
            if (isCacheValid(databaseCacheTimestamps.get(cacheKey))) {
                List<String> cached = databaseTablesCache.get(cacheKey);
                if (cached != null) {
                    log.debug("Retrieved databases from cache: {}", cached);
                    return cached;
                }
            }

            // Get databases from Fluss admin client
            Admin admin = connectionManager.getAdmin();
            List<String> databases = admin.listDatabases().get();

            // Update cache
            if (metadataCacheTtl > 0) {
                databaseTablesCache.put(cacheKey, databases);
                databaseCacheTimestamps.put(cacheKey, System.currentTimeMillis());
            }

            log.debug("Listed databases: {}", databases);
            return databases;

        } catch (Exception e) {
            throw new CatalogException("Failed to list databases", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            String normalizedName = normalizeName(databaseName);

            if (!databaseExists(normalizedName)) {
                throw new DatabaseNotExistException(catalogName, normalizedName);
            }

            // Check cache first
            if (isCacheValid(databaseCacheTimestamps.get(normalizedName))) {
                List<String> cached = databaseTablesCache.get(normalizedName);
                if (cached != null) {
                    log.debug("Retrieved tables for database '{}' from cache: {}", normalizedName, cached);
                    return cached;
                }
            }

            // Get tables from Fluss admin client
            Admin admin = connectionManager.getAdmin();
            List<String> tables = admin.listTables(normalizedName).get();

            // Update cache
            if (metadataCacheTtl > 0) {
                databaseTablesCache.put(normalizedName, tables);
                databaseCacheTimestamps.put(normalizedName, System.currentTimeMillis());
            }

            log.debug("Listed tables for database '{}': {}", normalizedName, tables);
            return tables;

        } catch (DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list tables in database '%s'", databaseName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            TablePath normalizedPath = normalizeTablePath(tablePath);

            // Check if database exists first
            if (!databaseExists(normalizedPath.getDatabaseName())) {
                return false;
            }

            // Check if table exists using Fluss admin client
            Admin admin = connectionManager.getAdmin();
            com.alibaba.fluss.metadata.TablePath flussTablePath =
                    com.alibaba.fluss.metadata.TablePath.of(
                            normalizedPath.getDatabaseName(),
                            normalizedPath.getTableName());

            return admin.tableExists(flussTablePath).get();

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to check if table '%s' exists", tablePath), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        try {
            TablePath normalizedPath = normalizeTablePath(tablePath);

            // Check cache first
            if (isCacheValid(tableCacheTimestamps.get(normalizedPath))) {
                CatalogTable cached = tableMetadataCache.get(normalizedPath);
                if (cached != null) {
                    log.debug("Retrieved table metadata for '{}' from cache", normalizedPath);
                    return cached;
                }
            }

            if (!tableExists(normalizedPath)) {
                throw new TableNotExistException(catalogName, normalizedPath);
            }

            // Get table metadata from Fluss admin client
            Admin admin = connectionManager.getAdmin();
            com.alibaba.fluss.metadata.TablePath flussTablePath =
                    com.alibaba.fluss.metadata.TablePath.of(
                            normalizedPath.getDatabaseName(),
                            normalizedPath.getTableName());

            TableDescriptor tableDescriptor = admin.getTable(flussTablePath).get();

            // Convert Fluss table descriptor to SeaTunnel CatalogTable
            CatalogTable catalogTable = FlussTypeConverter.toSeaTunnelTable(tableDescriptor);

            // Update cache
            if (metadataCacheTtl > 0) {
                tableMetadataCache.put(normalizedPath, catalogTable);
                tableCacheTimestamps.put(normalizedPath, System.currentTimeMillis());
            }

            log.debug("Retrieved table metadata for '{}': {}", normalizedPath, catalogTable.getTableId());
            return catalogTable;

        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to get table metadata for '%s'", tablePath), e);
        }
    }

    // Helper methods
    
    private String normalizeName(String name) {
        return caseSensitive ? name : name.toLowerCase();
    }
    
    private TablePath normalizeTablePath(TablePath tablePath) {
        return TablePath.of(
                normalizeName(tablePath.getDatabaseName()),
                normalizeName(tablePath.getTableName()));
    }
    
    private boolean isCacheValid(@Nullable Long timestamp) {
        if (metadataCacheTtl <= 0 || timestamp == null) {
            return false;
        }
        return System.currentTimeMillis() - timestamp < metadataCacheTtl;
    }

    // Unsupported operations (for now)
    
    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new FlussConnectorException(
                FlussConnectorErrorCode.UNSUPPORTED_OPERATION,
                "createTable operation is not yet implemented");
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new FlussConnectorException(
                FlussConnectorErrorCode.UNSUPPORTED_OPERATION,
                "dropTable operation is not yet implemented");
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new FlussConnectorException(
                FlussConnectorErrorCode.UNSUPPORTED_OPERATION,
                "createDatabase operation is not yet implemented");
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new FlussConnectorException(
                FlussConnectorErrorCode.UNSUPPORTED_OPERATION,
                "dropDatabase operation is not yet implemented");
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new FlussConnectorException(
                FlussConnectorErrorCode.UNSUPPORTED_OPERATION,
                "truncateTable operation is not yet implemented");
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        // TODO: Implement actual check for data existence
        return false;
    }
}
