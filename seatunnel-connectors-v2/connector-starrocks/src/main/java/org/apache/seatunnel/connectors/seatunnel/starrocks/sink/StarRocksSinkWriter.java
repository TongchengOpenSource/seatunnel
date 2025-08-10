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

import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.writer.LabelGenerator;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.writer.StarRocksSinkState;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

@Slf4j
public class StarRocksSinkWriter
        implements SinkWriter<SeaTunnelRow, StarRocksCommitInfo, StarRocksSinkState>,
                SupportMultiTableSinkWriter<StarRocksCommitInfo>,
                SupportSchemaEvolutionSinkWriter {
    private static final int INITIAL_DELAY = 200;
    private long lastCheckpointId;
    private StarRocksISerializer serializer;
    private StarRocksSinkManager manager;
    private StarRocksTransactionSinkManager transactionManager;
    private TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final TablePath sinkTablePath;
    private final TableSchemaChangeEventDispatcher tableSchemaChangeEventDispatcher =
            new TableSchemaChangeEventDispatcher();
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final int intervalTime;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile Exception loadException;

    public StarRocksSinkWriter(
            SinkWriter.Context context,
            List<StarRocksSinkState> state,
            SinkConfig sinkConfig,
            TableSchema tableSchema,
            TablePath tablePath,
            String jobId) {
        this.lastCheckpointId = !state.isEmpty() ? state.get(0).getCheckpointId() : 0;
        log.info("restore checkpointId {}", lastCheckpointId);
        this.tableSchema = tableSchema;
        SeaTunnelRowType seaTunnelRowType = tableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.sinkConfig = sinkConfig;
        this.sinkTablePath = tablePath;

        // Generate labelPrefix similar to DorisSinkWriter
        this.labelPrefix =
                (sinkConfig.getLabelPrefix() != null ? sinkConfig.getLabelPrefix() : "starrocks")
                        + "_"
                        + tablePath.getFullName().replaceAll("\\.", "_")
                        + "_"
                        + jobId
                        + "_"
                        + context.getIndexOfSubtask();

        this.labelGenerator = new LabelGenerator(labelPrefix, sinkConfig.isEnable2PC());
        this.intervalTime = 5000; // Default check interval 5 seconds
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder().setNameFormat("starrocks-load-check").build());

        // Initialize appropriate manager based on 2PC configuration
        if (sinkConfig.isEnable2PC()) {
            this.transactionManager = new StarRocksTransactionSinkManager(sinkConfig);
        } else {
            this.manager = new StarRocksSinkManager(sinkConfig, tableSchema);
        }

        this.initializeLoad();
    }

    private void initializeLoad() {
        startLoad(labelGenerator.generateLabel(lastCheckpointId + 1));
        // Start periodic check for load exceptions
        scheduledExecutorService.scheduleWithFixedDelay(
                this::checkDone, INITIAL_DELAY, intervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkLoadException();
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
    public List<StarRocksSinkState> snapshotState(long checkpointId) throws IOException {
        // For 2PC mode, ensure transaction manager exists
        if (sinkConfig.isEnable2PC()) {
            checkState(transactionManager != null);
        } else {
            checkState(manager != null);
        }
        startLoad(labelGenerator.generateLabel(checkpointId + 1));
        this.lastCheckpointId = checkpointId;
        return Collections.singletonList(new StarRocksSinkState(labelPrefix, lastCheckpointId));
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
            // For non-2PC mode, flush remaining data before closing
            if (!sinkConfig.isEnable2PC() && manager != null) {
                manager.flush();
            }

            if (sinkConfig.isEnable2PC() && transactionManager != null) {
                transactionManager.close();
            } else if (manager != null) {
                manager.close();
            }

            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (IOException e) {
            log.error("Close starRocks manager failed.", e);
            throw CommonError.closeFailed(StarRocksBaseOptions.CONNECTOR_IDENTITY, e);
        }
    }

    private void startLoad(String label) {
        log.info("Starting load with label: {}", label);
        // For StarRocks, we don't need to explicitly start a load like Doris
        // The load is handled by the managers when data is written
    }

    private void checkDone() {
        // Check for load exceptions periodically
        log.debug("start timer checker, interval {} ms", intervalTime);
        // For StarRocks, exception handling is done within the managers
        // This method is kept for consistency with DorisSinkWriter pattern
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new RuntimeException("error while loading data.", loadException);
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
