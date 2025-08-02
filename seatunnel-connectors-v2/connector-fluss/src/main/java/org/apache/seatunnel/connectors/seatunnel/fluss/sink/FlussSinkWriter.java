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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.fluss.client.FlussConnectionManager;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.row.GenericRow;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class FlussSinkWriter implements SinkWriter<SeaTunnelRow, FlussCommitInfo, FlussSinkState> {

    private final SinkWriter.Context context;
    private final FlussSinkConfig sinkConfig;
    private final SeaTunnelRowType rowType;
    private final List<SeaTunnelRow> bufferedRows;

    // Fluss client components
    private FlussConnectionManager connectionManager;
    private Table flussTable;
    private UpsertWriter upsertWriter;
    private AppendWriter appendWriter;

    private long lastFlushTime;
    private boolean isUpsertMode;

    public FlussSinkWriter(
            SinkWriter.Context context,
            FlussSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            List<FlussSinkState> states) {
        this.context = context;
        this.sinkConfig = sinkConfig;
        this.rowType = rowType;
        this.bufferedRows = new ArrayList<>();
        this.lastFlushTime = System.currentTimeMillis();
        this.isUpsertMode = "upsert".equals(sinkConfig.getWriteMode());

        try {
            initializeFlussClient();
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to initialize Fluss sink writer",
                    e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            bufferedRows.add(element);
            if (shouldFlush()) {
                flush(false);
            }
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.WRITE_DATA_FAILED, "Failed to write data to Fluss", e);
        }
    }

    @Override
    public Optional<FlussCommitInfo> prepareCommit() throws IOException {
        try {
            // Flush any remaining data
            flush(true);

            if (sinkConfig.getEnableTransaction()) {
                // For transactional writes, return commit info
                return Optional.of(
                        new FlussCommitInfo("transaction-" + System.currentTimeMillis()));
            }

            return Optional.empty();
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.TRANSACTION_FAILED, "Failed to prepare commit", e);
        }
    }

    @Override
    public List<FlussSinkState> snapshotState(long checkpointId) throws IOException {
        try {
            FlussSinkState state = new FlussSinkState();
            if (sinkConfig.getEnableTransaction()) {
                state.setTransactionIds(Collections.singletonList(getCurrentTransactionId()));
            }
            state.setLastCheckpointId(checkpointId);

            return Collections.singletonList(state);
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INTERNAL_ERROR, "Failed to snapshot state", e);
        }
    }

    @Override
    public void abortPreparedTransactions(List<FlussCommitInfo> commitInfos) throws IOException {
        try {
            for (FlussCommitInfo commitInfo : commitInfos) {
                log.debug("Aborting transaction: {}", commitInfo.getTransactionId());
                // In a real implementation, this would abort the specific transaction
            }
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.TRANSACTION_FAILED, "Failed to abort transactions", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // Flush any remaining data
            if (!bufferedRows.isEmpty()) {
                flush(true);
            }

            // Close writers
            if (upsertWriter != null) {
                upsertWriter.close();
            }
            if (appendWriter != null) {
                appendWriter.close();
            }

            // Close connection manager
            if (connectionManager != null) {
                connectionManager.close();
            }

            log.info("Fluss sink writer closed");
        } catch (Exception e) {
            log.warn("Error closing Fluss sink writer", e);
        }
    }

    private void initializeFlussClient() throws Exception {
        log.info("Initializing Fluss client for sink");

        connectionManager = new FlussConnectionManager(sinkConfig.toFlussProperties());
        flussTable = connectionManager.getTable(sinkConfig.getDatabase(), sinkConfig.getTable());
        if (isUpsertMode) {
            upsertWriter = flussTable.newUpsert().createWriter();
            log.info("Created upsert writer for table {}", sinkConfig.getFullTableName());
        } else {
            appendWriter = flussTable.newAppend().createWriter();
            log.info("Created append writer for table {}", sinkConfig.getFullTableName());
        }
    }

    private boolean shouldFlush() {
        long currentTime = System.currentTimeMillis();
        return bufferedRows.size() >= sinkConfig.getBatchSize()
                || (currentTime - lastFlushTime) >= sinkConfig.getBatchTimeoutMs();
    }

    private void flush(boolean force) throws Exception {
        if (bufferedRows.isEmpty() && !force) {
            return;
        }

        log.debug("Flushing {} rows to Fluss", bufferedRows.size());

        try {
            // Convert and send rows to Fluss
            for (SeaTunnelRow row : bufferedRows) {
                GenericRow flussRow = FlussTypeConverter.convertToFlussRow(row, rowType);

                if (isUpsertMode && upsertWriter != null) {
                    upsertWriter.upsert(flussRow);
                } else if (!isUpsertMode && appendWriter != null) {
                    appendWriter.append(flussRow);
                }
            }

            // Flush the writer
            if (isUpsertMode && upsertWriter != null) {
                upsertWriter.flush();
            } else if (!isUpsertMode && appendWriter != null) {
                appendWriter.flush();
            }

            // Clear buffer and update flush time
            bufferedRows.clear();
            lastFlushTime = System.currentTimeMillis();

            log.debug("Successfully flushed data to Fluss");

        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.WRITE_DATA_FAILED, "Failed to flush data to Fluss", e);
        }
    }
}
