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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksTransactionSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksCsvSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksISerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksJsonSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.starrocks.util.SchemaUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class StarRocksSinkWriter
        implements SinkWriter<SeaTunnelRow, StarRocksCommitInfo, Void>,
                SupportMultiTableSinkWriter<StarRocksCommitInfo>,
                SupportSchemaEvolutionSinkWriter {
    private StarRocksISerializer serializer;
    private StarRocksSinkManager manager;
    private StarRocksTransactionSinkManager transactionManager;
    private TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final TablePath sinkTablePath;
    private final TableSchemaChangeEventDispatcher tableSchemaChangeEventDispatcher =
            new TableSchemaChangeEventDispatcher();

    public StarRocksSinkWriter(
            SinkConfig sinkConfig, TableSchema tableSchema, TablePath tablePath) {
        this.tableSchema = tableSchema;
        SeaTunnelRowType seaTunnelRowType = tableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.sinkConfig = sinkConfig;
        this.sinkTablePath = tablePath;

        // Initialize appropriate manager based on 2PC configuration
        if (sinkConfig.isEnable2PC()) {
            this.transactionManager = new StarRocksTransactionSinkManager(sinkConfig);
        } else {
            this.manager = new StarRocksSinkManager(sinkConfig, tableSchema);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record;
        try {
            record = serializer.serialize(element);
        } catch (Exception e) {
            throw CommonError.seatunnelRowSerializeFailed(element.toString(), e);
        }

        if (sinkConfig.isEnable2PC()) {
            transactionManager.write(record);
        } else {
            manager.write(record);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        this.tableSchema = tableSchemaChangeEventDispatcher.reset(tableSchema).apply(event);
        SeaTunnelRowType seaTunnelRowType = tableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);

        // Reinitialize appropriate manager based on 2PC configuration
        if (sinkConfig.isEnable2PC()) {
            if (this.transactionManager != null) {
                try {
                    this.transactionManager.close();
                } catch (IOException e) {
                    log.warn("Error closing previous transaction manager", e);
                }
            }
            this.transactionManager = new StarRocksTransactionSinkManager(sinkConfig);
        } else {
            this.manager = new StarRocksSinkManager(sinkConfig, tableSchema);
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load MySQL JDBC driver", e);
        }

        try (Connection conn =
                DriverManager.getConnection(
                        sinkConfig.getJdbcUrl(),
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword())) {
            SchemaUtils.applySchemaChange(event, conn, sinkTablePath);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", sinkConfig.getJdbcUrl()), e);
        }
    }

    @SneakyThrows
    @Override
    public Optional<StarRocksCommitInfo> prepareCommit() {
        if (sinkConfig.isEnable2PC()) {
            // For transaction mode, prepare commit and return commit info
            StarRocksCommitInfo commitInfo = transactionManager.prepareCommit();
            return Optional.ofNullable(commitInfo);
        } else {
            // For non-transaction mode, just flush
            manager.flush();
            return Optional.empty();
        }
    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {
        if (sinkConfig.isEnable2PC() && transactionManager != null) {
            transactionManager.abortTransaction();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (sinkConfig.isEnable2PC() && transactionManager != null) {
                transactionManager.close();
            } else if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            log.error("Close starRocks manager failed.", e);
            throw CommonError.closeFailed(StarRocksBaseOptions.CONNECTOR_IDENTITY, e);
        }
    }

    public StarRocksISerializer createSerializer(
            SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksCsvSerializer(
                    sinkConfig.getColumnSeparator(),
                    seaTunnelRowType,
                    sinkConfig.isEnableUpsertDelete());
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksJsonSerializer(seaTunnelRowType, sinkConfig.isEnableUpsertDelete());
        }
        throw CommonError.illegalArgument(
                sinkConfig.getLoadFormat().name(), "starrocks stream load");
    }
}
