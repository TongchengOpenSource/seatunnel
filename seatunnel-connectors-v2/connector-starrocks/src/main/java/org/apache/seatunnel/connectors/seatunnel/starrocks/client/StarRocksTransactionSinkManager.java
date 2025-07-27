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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** StarRocks transaction sink manager for handling transaction stream load */
@Slf4j
public class StarRocksTransactionSinkManager {

    private final SinkConfig sinkConfig;
    private final List<byte[]> batchList;
    private final List<String> availableHosts;

    private StarRocksTransactionStreamLoad currentStreamLoad;
    private volatile boolean initialize;
    private volatile Exception flushException;
    private int batchRowCount = 0;
    private long batchBytesSize = 0;
    private int currentHostIndex = 0;

    public StarRocksTransactionSinkManager(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();
        this.availableHosts = new ArrayList<>(sinkConfig.getNodeUrls());
        Collections.shuffle(this.availableHosts);
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }
        initialize = true;
    }

    public synchronized void write(String record) throws IOException {
        tryInit();
        checkFlushException();

        byte[] bts = record.getBytes(StandardCharsets.UTF_8);
        batchList.add(bts);
        batchRowCount++;
        batchBytesSize += bts.length;

        if (batchRowCount >= sinkConfig.getBatchMaxSize()
                || batchBytesSize >= sinkConfig.getBatchMaxBytes()) {
            flush();
        }
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }

        if (currentStreamLoad == null) {
            String label = createBatchLabel();
            String hostPort = getAvailableHost();
            currentStreamLoad = new StarRocksTransactionStreamLoad(sinkConfig, hostPort, label);
        }

        // Write all records to the transaction stream load
        for (byte[] record : batchList) {
            currentStreamLoad.writeRecord(record);
        }

        // Load data to StarRocks
        currentStreamLoad.loadData();

        // Clear batch
        batchList.clear();
        batchRowCount = 0;
        batchBytesSize = 0;
    }

    /** Prepare commit for checkpoint */
    public synchronized StarRocksCommitInfo prepareCommit() throws IOException {
        // Flush any remaining data
        flush();

        if (currentStreamLoad != null && currentStreamLoad.isTransactionStarted()) {
            // Prepare the transaction
            currentStreamLoad.prepareCommit();

            // Get commit info
            StarRocksCommitInfo commitInfo = currentStreamLoad.getCommitInfo();

            // Reset current stream load for next checkpoint
            currentStreamLoad = null;

            return commitInfo;
        }

        return null;
    }

    public synchronized void close() throws IOException {
        try {
            flush();
            if (currentStreamLoad != null) {
                currentStreamLoad.close();
            }
        } catch (Exception e) {
            log.error("Error closing StarRocksTransactionSinkManager", e);
            throw e;
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, flushException);
        }
    }

    private String createBatchLabel() {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(sinkConfig.getLabelPrefix())) {
            sb.append(sinkConfig.getLabelPrefix());
        }
        return sb.append(UUID.randomUUID()).toString();
    }

    private String getAvailableHost() {
        if (availableHosts.isEmpty()) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_IS_NULL,
                    "No available hosts for StarRocks connection");
        }

        String host = availableHosts.get(currentHostIndex % availableHosts.size());
        currentHostIndex++;
        return host;
    }

    /** Abort current transaction if exists */
    public synchronized void abortTransaction() {
        if (currentStreamLoad != null) {
            try {
                currentStreamLoad.close();
            } catch (IOException e) {
                log.warn("Error closing transaction stream load during abort", e);
            } finally {
                currentStreamLoad = null;
            }
        }

        // Clear batch data
        batchList.clear();
        batchRowCount = 0;
        batchBytesSize = 0;
    }
}
