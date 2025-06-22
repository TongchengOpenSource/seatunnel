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

package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceTableConfig;

import java.util.List;
import java.util.stream.Collectors;

public class FlussSource
        implements SeaTunnelSource<SeaTunnelRow, FlussSourceSplit, FlussSourceState> {

    private SourceConfig sourceConfig;

    @Override
    public String getPluginName() {
        return FlussBaseOptions.CONNECTOR_IDENTITY;
    }

    public FlussSource(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return sourceConfig.getTableConfigList().stream()
                .map(FlussSourceTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) {
        return new FlussSourceReader(readerContext, sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<FlussSourceSplit, FlussSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FlussSourceSplit> enumeratorContext,
            FlussSourceState checkpointState)
            throws Exception {
        return new FlussSourceSplitEnumerator(
                enumeratorContext, sourceConfig, checkpointState);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) {
        return new FlussSourceSplitEnumerator(enumeratorContext, sourceConfig);
    }
}
