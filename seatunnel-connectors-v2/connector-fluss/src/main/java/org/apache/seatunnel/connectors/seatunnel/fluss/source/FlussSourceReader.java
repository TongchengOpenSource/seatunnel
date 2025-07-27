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
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.fluss.client.FlussConnectionManager;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fluss.util.FlussTypeConverter;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.ListOffsetsResult;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Slf4j
public class FlussSourceReader implements SourceReader<SeaTunnelRow, FlussSourceSplit> {

    private final SourceReader.Context context;
    private final FlussSourceConfig sourceConfig;
    private final SeaTunnelRowType rowType;
    private final Deque<FlussSourceSplit> pendingSplits;
    private final List<FlussSourceSplit> assignedSplits;

    private FlussConnectionManager connectionManager;
    private Table flussTable;
    private LogScanner logScanner;
    private TablePath tablePath;
    ;

    private volatile boolean isRunning = true;

    public FlussSourceReader(
            SourceReader.Context context,
            FlussSourceConfig sourceConfig,
            SeaTunnelRowType rowType) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.rowType = rowType;
        this.pendingSplits = new ConcurrentLinkedDeque<>();
        this.assignedSplits = new ArrayList<>();
        this.tablePath = TablePath.of(sourceConfig.getDatabase(), sourceConfig.getTable());
    }

    @Override
    public void open() throws Exception {
        log.info("Opening Fluss source reader");
        try {
            initializeFlussClient();
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to initialize Fluss client",
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        log.info("Closing Fluss source reader");
        isRunning = false;
        try {
            if (logScanner != null) {
                logScanner.close();
            }
            if (connectionManager != null) {
                connectionManager.close();
            }
        } catch (Exception e) {
            log.warn("Error closing Fluss client", e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (logScanner == null) {
            Thread.sleep(sourceConfig.getPollTimeoutMs());
            return;
        }

        try {
            ScanRecords scanRecords =
                    logScanner.poll(Duration.ofMillis(sourceConfig.getPollTimeoutMs()));

            if (scanRecords.isEmpty()) {
                if (context.getBoundedness() == Boundedness.BOUNDED) {
                    boolean allFinished =
                            pendingSplits.stream().allMatch(FlussSourceSplit::isFinished);
                    if (allFinished) {
                        context.signalNoMoreElement();
                    }
                }
                return;
            }

            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    InternalRow flussRow = record.getRow();
                    SeaTunnelRow seaTunnelRow =
                            FlussTypeConverter.convertFromFlussRow(flussRow, rowType);
                    output.collect(seaTunnelRow);
                }
            }

            log.debug("Processed {} records from Fluss", scanRecords.count());

        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.READ_DATA_FAILED, "Failed to read data from Fluss", e);
        }
    }

    @Override
    public List<FlussSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(assignedSplits);
    }

    @Override
    public void addSplits(List<FlussSourceSplit> splits) {
        log.info("Adding {} splits to reader", splits.size());
        pendingSplits.addAll(splits);
        assignedSplits.addAll(splits);

        if (logScanner != null) {
            subscribeToSplitsWithOffsets(splits);
        }
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("No more splits will be assigned to this reader");
        // Mark that no more splits will be assigned
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Handle checkpoint completion if needed
        log.debug("Checkpoint {} completed", checkpointId);
    }

    private void initializeFlussClient() throws Exception {
        log.info(
                "Initializing Fluss client with bootstrap servers: {}",
                sourceConfig.getBootstrapServers());

        connectionManager = new FlussConnectionManager(sourceConfig.toFlussProperties());
        flussTable =
                connectionManager.getTable(sourceConfig.getDatabase(), sourceConfig.getTable());
        logScanner = flussTable.newScan().createLogScanner();

        log.info(
                "Successfully initialized Fluss client for table {}",
                sourceConfig.getFullTableName());
    }


    private void subscribeToSplitsWithOffsets(List<FlussSourceSplit> splits) {
        try {
            StartupMode startupMode = sourceConfig.getScanStartupMode();

            switch (startupMode) {
                case EARLIEST:
                    subscribeFromEarliestOffsets(splits);
                    break;
                case LATEST:
                    subscribeFromLatestOffsets(splits);
                    break;
                case TIMESTAMP:
                    if (sourceConfig.getScanStartupTimestamp() != null) {
                        subscribeFromTimestampOffsets(
                                splits, sourceConfig.getScanStartupTimestamp());
                    } else {
                        throw new FlussConnectorException(
                                FlussConnectorErrorCode.INVALID_CONFIGURATION,
                                "Timestamp startup mode specified but scan.startup.timestamp is not provided. "
                                        + "Please provide a valid timestamp value when using timestamp startup mode.");
                    }
                    break;
                default:
                    throw new FlussConnectorException(
                            FlussConnectorErrorCode.INVALID_CONFIGURATION,
                            "Unsupported startup mode: " + startupMode);
            }
        } catch (Exception e) {
            log.error("Failed to subscribe to splits with offsets", e);
            throw new FlussConnectorException(FlussConnectorErrorCode.READ_DATA_FAILED, e);
        }
    }

    private void subscribeFromEarliestOffsets(List<FlussSourceSplit> splits) throws Exception {
        for (FlussSourceSplit split : splits) {
            logScanner.subscribeFromBeginning(split.getBucketId());
            log.info(
                    "Subscribed to bucket {} for split {} from beginning",
                    split.getBucketId(),
                    split.splitId());
        }
    }

    private void subscribeFromLatestOffsets(List<FlussSourceSplit> splits) throws Exception {
        Admin admin = connectionManager.getAdmin();

        List<Integer> buckets =
                splits.stream().map(FlussSourceSplit::getBucketId).collect(Collectors.toList());

        try {
            ListOffsetsResult result =
                    admin.listOffsets(tablePath, buckets, new OffsetSpec.LatestSpec());
            Map<Integer, Long> latestOffsets = result.all().get();
            for (FlussSourceSplit split : splits) {
                Long latestOffset = latestOffsets.get(split.getBucketId());

                if (latestOffset != null) {
                    logScanner.subscribe(split.getBucketId(), latestOffset);
                    log.info(
                            "Subscribed to bucket {} for split {} from latest offset {}",
                            split.getBucketId(),
                            split.splitId(),
                            latestOffset);
                }
            }
        } catch (Exception e) {
            throw new FlussConnectorException(FlussConnectorErrorCode.READ_DATA_FAILED, e);
        }
    }

    private void subscribeFromTimestampOffsets(List<FlussSourceSplit> splits, long timestamp)
            throws Exception {
        Admin admin = connectionManager.getAdmin();
        List<Integer> buckets =
                splits.stream().map(FlussSourceSplit::getBucketId).collect(Collectors.toList());

        try {
            ListOffsetsResult result =
                    admin.listOffsets(tablePath, buckets, new OffsetSpec.TimestampSpec(timestamp));
            Map<Integer, Long> timestampOffsets = result.all().get();

            for (FlussSourceSplit split : splits) {
                Long timestampOffset = timestampOffsets.get(split.getBucketId());

                if (timestampOffset != null) {
                    logScanner.subscribe(split.getBucketId(), timestampOffset);
                    log.debug(
                            "Subscribed to bucket {} for split {} from timestamp {} with offset {}",
                            split.getBucketId(),
                            split.splitId(),
                            timestamp,
                            timestampOffset);
                }
            }
        } catch (Exception e) {
            log.error(
                    "Failed to get timestamp offsets from Admin, falling back to earliest mode", e);
            throw new FlussConnectorException(FlussConnectorErrorCode.READ_DATA_FAILED, e);
        }
    }
}
