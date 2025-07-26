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

package org.apache.seatunnel.connectors.seatunnel.fluss.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussOptions;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class FlussSink implements SeaTunnelSink<SeaTunnelRow, FlussSinkState, FlussCommitInfo, FlussAggregatedCommitInfo> {

    private final FlussSinkConfig sinkConfig;
    private final SeaTunnelRowType rowType;
    private final CatalogTable catalogTable;

    public FlussSink(FlussSinkConfig sinkConfig, CatalogTable catalogTable) {
        this.sinkConfig = sinkConfig;
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public String getPluginName() {
        return FlussOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public SinkWriter<SeaTunnelRow, FlussCommitInfo, FlussSinkState> createWriter(SinkWriter.Context context)
            throws IOException {
        return new FlussSinkWriter(context, sinkConfig, rowType, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, FlussCommitInfo, FlussSinkState> restoreWriter(
            SinkWriter.Context context, List<FlussSinkState> states) throws IOException {
        return new FlussSinkWriter(context, sinkConfig, rowType, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FlussCommitInfo, FlussAggregatedCommitInfo>> createAggregatedCommitter()
            throws IOException {
        if (sinkConfig.getEnableTransaction()) {
            return Optional.of(new FlussAggregatedCommitter(sinkConfig));
        }
        return Optional.empty();
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
