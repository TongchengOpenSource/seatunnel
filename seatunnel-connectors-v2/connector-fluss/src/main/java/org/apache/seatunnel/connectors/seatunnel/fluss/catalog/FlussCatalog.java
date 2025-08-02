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
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussOptions;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.IOUtils;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fluss catalog implementation for SeaTunnel, following the official Fluss Flink connector design.
 *
 * <p>This catalog provides metadata operations for Fluss tables, including:
 *
 * <ul>
 *   <li>Database operations: list, create, drop, check existence
 *   <li>Table operations: list, create, drop, get metadata, check existence
 * </ul>
 *
 * <p>The catalog follows the SeaTunnel catalog interface and integrates with Fluss admin client for
 * metadata operations.
 */
@Slf4j
public class FlussCatalog implements Catalog {

    private final String catalogName;
    private final String defaultDatabase;
    private final String bootstrapServers;

    private Connection connection;
    private Admin admin;

    public FlussCatalog(String catalogName, ReadonlyConfig config) {
        this.catalogName = catalogName;
        this.defaultDatabase = config.get(FlussCatalogOptions.DEFAULT_DATABASE);
        this.bootstrapServers = config.get(FlussOptions.BOOTSTRAP_SERVERS);

        log.info(
                "Created Fluss catalog '{}' with default database '{}'",
                catalogName,
                defaultDatabase);
    }

    @Override
    public void open() throws CatalogException {
        try {
            Map<String, String> flussConfigs = new HashMap<>();
            flussConfigs.put("bootstrap.servers", bootstrapServers);

            connection = ConnectionFactory.createConnection(Configuration.fromMap(flussConfigs));
            admin = connection.getAdmin();

            log.info("Opened Fluss catalog '{}' successfully", catalogName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to open Fluss catalog '%s'", catalogName),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public void close() throws CatalogException {
        try {
            IOUtils.closeQuietly(admin, "fluss-admin");
            IOUtils.closeQuietly(connection, "fluss-connection");

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

    @Nullable @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return admin.databaseExists(databaseName).get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to check if database '%s' exists in %s", databaseName, name()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return admin.listDatabases().get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list all databases in %s", name()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            return admin.listTables(databaseName).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isDatabaseNotExist(t)) {
                throw new DatabaseNotExistException(name(), databaseName);
            }
            throw new CatalogException(
                    String.format(
                            "Failed to list all tables in database %s in %s", databaseName, name()),
                    t);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            com.alibaba.fluss.metadata.TablePath flussTablePath = toFlussTablePath(tablePath);
            return admin.tableExists(flussTablePath).get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to check if table %s exists in %s", tablePath, name()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        try {
            com.alibaba.fluss.metadata.TablePath flussTablePath = toFlussTablePath(tablePath);

            if (!tableExists(tablePath)) {
                throw new TableNotExistException(name(), tablePath);
            }
            TableInfo tableInfo = admin.getTableInfo(flussTablePath).get();
            CatalogTable catalogTable = FlussTypeConverter.toSeaTunnelTable(tableInfo);
            log.debug(
                    "Retrieved table metadata for '{}': {}", tablePath, catalogTable.getTableId());
            return catalogTable;

        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new TableNotExistException(name(), tablePath);
            } else {
                throw new CatalogException(
                        String.format("Failed to get table %s in %s", tablePath, name()), t);
            }
        }
    }

    // Helper methods

    private com.alibaba.fluss.metadata.TablePath toFlussTablePath(TablePath tablePath) {
        return com.alibaba.fluss.metadata.TablePath.of(
                tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private boolean isDatabaseNotExist(Throwable t) {
        return t instanceof com.alibaba.fluss.exception.DatabaseNotExistException;
    }

    private boolean isTableNotExist(Throwable t) {
        return t instanceof com.alibaba.fluss.exception.TableNotExistException;
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
