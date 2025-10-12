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
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.SinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.DefaultSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.TransactionSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksCsvSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksISerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksJsonSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.commiter.StarRocksCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.state.StarRocksSinkState;
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

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED;

@Slf4j
public class StarRocksSinkWriter
        implements SinkWriter<SeaTunnelRow, StarRocksCommitInfo, StarRocksSinkState>,
                SupportMultiTableSinkWriter<StarRocksCommitInfo>,
                SupportSchemaEvolutionSinkWriter {
    private StarRocksISerializer serializer;
    private SinkManager manager;
    private TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final TablePath sinkTablePath;
    private long lastCheckpointId;
    private SinkWriter.Context context;
    private LabelGenerator labelGenerator;
    private final TableSchemaChangeEventDispatcher tableSchemaChangeEventDispatcher =
            new TableSchemaChangeEventDispatcher();

    public StarRocksSinkWriter(
            SinkWriter.Context context,
            List<StarRocksSinkState> state,
            SinkConfig sinkConfig,
            TableSchema tableSchema,
            TablePath tablePath) {
        this.lastCheckpointId = !state.isEmpty() ? state.get(0).getCheckpointId() : 0;
        log.info("restore checkpointId {}", lastCheckpointId);
        this.tableSchema = tableSchema;
        this.serializer = createSerializer(sinkConfig, tableSchema.toPhysicalRowDataType());
        this.sinkConfig = sinkConfig;
        this.sinkTablePath = tablePath;
        this.context = context;
        this.labelGenerator = new LabelGenerator(sinkConfig);
        this.manager =
                sinkConfig.isEnableExactlyOnce()
                        ? new TransactionSinkManager(labelGenerator, sinkConfig, context)
                        : new DefaultSinkManager(sinkConfig, tableSchema);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record;
        try {
            record = serializer.serialize(element);
        } catch (Exception e) {
            throw CommonError.seatunnelRowSerializeFailed(element.toString(), e);
        }
        manager.write(record);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        this.tableSchema = tableSchemaChangeEventDispatcher.reset(tableSchema).apply(event);
        SeaTunnelRowType seaTunnelRowType = tableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.manager =
                sinkConfig.isEnableExactlyOnce()
                        ? new TransactionSinkManager(labelGenerator, sinkConfig, context)
                        : new DefaultSinkManager(sinkConfig, tableSchema);

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
        if (!sinkConfig.isEnableExactlyOnce()) {
            return Optional.empty();
        }
        return manager.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        if (sinkConfig.isEnableExactlyOnce()) {
            try {
                manager.abort();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<StarRocksSinkState> snapshotState(long checkpointId) throws IOException {
        if (sinkConfig.isEnableExactlyOnce()) {
            return manager.snapshot(checkpointId);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (Exception e) {
            log.error("Close starRocks manager failed.", e);
            throw new StarRocksConnectorException(WRITER_OPERATION_FAILED, e);
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
