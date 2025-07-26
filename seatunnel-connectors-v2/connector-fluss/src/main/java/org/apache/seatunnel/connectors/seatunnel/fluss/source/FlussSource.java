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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussOptions;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class FlussSource implements SeaTunnelSource<SeaTunnelRow, FlussSourceSplit, FlussSourceState>, SupportParallelism {

    private final FlussSourceConfig sourceConfig;
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public FlussSource(ReadonlyConfig config, CatalogTable catalogTable) {
        this.sourceConfig = new FlussSourceConfig(config);
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return FlussOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Boundedness getBoundedness() {
        // Determine boundedness based on job mode and configuration
        if (jobContext != null && JobMode.BATCH.equals(jobContext.getJobMode())) {
            return Boundedness.BOUNDED;
        }
        
        // For streaming mode, check if changelog is enabled
        if (sourceConfig.getEnableChangelog()) {
            return Boundedness.UNBOUNDED;
        }
        
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, FlussSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new FlussSourceReader(readerContext, sourceConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<FlussSourceSplit, FlussSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FlussSourceSplit> enumeratorContext) throws Exception {
        return new FlussSourceSplitEnumerator(enumeratorContext, sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<FlussSourceSplit, FlussSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FlussSourceSplit> enumeratorContext,
            FlussSourceState checkpointState) throws Exception {
        return new FlussSourceSplitEnumerator(enumeratorContext, sourceConfig, checkpointState);
    }
}
