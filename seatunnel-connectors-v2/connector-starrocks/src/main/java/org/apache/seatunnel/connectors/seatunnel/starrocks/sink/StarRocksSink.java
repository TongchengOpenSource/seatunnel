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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfoSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitter;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.writer.StarRocksSinkState;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.writer.StarRocksSinkStateSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.writer.StarRocksSinkWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class StarRocksSink
        implements SeaTunnelSink<
        SeaTunnelRow, StarRocksSinkState, StarRocksCommitInfo, StarRocksCommitInfo>,
        SupportSaveMode,
        SupportSchemaEvolutionSink,
        SupportMultiTableSink {

    private final TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final DataSaveMode dataSaveMode;
    private final SchemaSaveMode schemaSaveMode;
    private final CatalogTable catalogTable;
    private String jobId;

    public StarRocksSink(SinkConfig sinkConfig, CatalogTable catalogTable) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = catalogTable.getTableSchema();
        this.catalogTable = catalogTable;
        this.dataSaveMode = sinkConfig.getDataSaveMode();
        this.schemaSaveMode = sinkConfig.getSchemaSaveMode();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public String getPluginName() {
        return StarRocksCatalogFactory.IDENTIFIER;
    }

    @Override
    public StarRocksSinkWriter createWriter(SinkWriter.Context context) {
        TablePath sinkTablePath = catalogTable.getTablePath();
        return new StarRocksSinkWriter(
                context, Collections.emptyList(), sinkConfig, tableSchema, sinkTablePath, jobId);
    }

    @Override
    public SinkWriter<SeaTunnelRow, StarRocksCommitInfo, StarRocksSinkState> restoreWriter(
            SinkWriter.Context context, List<StarRocksSinkState> states) throws IOException {
        TablePath sinkTablePath = catalogTable.getTablePath();
        return new StarRocksSinkWriter(
                context, states, sinkConfig, tableSchema, sinkTablePath, jobId);
    }

    @Override
    public Optional<Serializer<StarRocksSinkState>> getWriterStateSerializer() {
        return Optional.of(new StarRocksSinkStateSerializer());
    }

    @Override
    public Optional<Serializer<StarRocksCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new StarRocksCommitInfoSerializer());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        TablePath tablePath =
                TablePath.of(
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        catalogTable.getTableId().getTableName());
        Catalog catalog =
                new StarRocksCatalog(
                        StarRocksBaseOptions.CONNECTOR_IDENTITY,
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword(),
                        sinkConfig.getJdbcUrl(),
                        sinkConfig.getSaveModeCreateTemplate());
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        catalog,
                        tablePath,
                        catalogTable,
                        sinkConfig.getCustomSql()));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public Optional<SinkCommitter<StarRocksCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new StarRocksCommitter(sinkConfig));
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }
}
