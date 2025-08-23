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

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksDelimiterParser;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Abstract base class for StarRocks stream load visitors */
@Slf4j
public abstract class AbstractStreamLoadVisitor {

    protected final SinkConfig sinkConfig;
    protected final TableSchema tableSchema;
    protected final HttpHelper httpHelper;

    protected static final int MAX_SLEEP_TIME = 5;
    protected static final String RESULT_FAILED = "Fail";
    protected static final String RESULT_SUCCESS = "Success";
    protected static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    protected static final String LABEL_STATE_VISIBLE = "VISIBLE";
    protected static final String LABEL_STATE_COMMITTED = "COMMITTED";
    protected static final String RESULT_LABEL_PREPARE = "PREPARE";
    protected static final String RESULT_LABEL_ABORTED = "ABORTED";
    protected static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    // Common state for host selection
    protected long pos = 0;

    public AbstractStreamLoadVisitor(SinkConfig sinkConfig, TableSchema tableSchema) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = tableSchema;
        this.httpHelper = new HttpHelper(sinkConfig);
    }

    /**
     * Execute stream load operation
     *
     * @param flushData the flush data containing batch, label, and size information
     * @return true if load is successful, false otherwise
     * @throws IOException if load fails
     */
    public abstract boolean doStreamLoad(StarRocksFlushTuple flushData) throws IOException;

    /**
     * Prepare commit for transaction mode (only applicable for transaction visitors)
     *
     * @return commit info for transaction, null for normal mode
     * @throws IOException if prepare fails
     */
    public StarRocksCommitInfo prepareCommit() throws IOException {
        // Default implementation for normal mode
        return null;
    }

    /**
     * Abort transaction (only applicable for transaction visitors)
     */
    public void abortTransaction() {
        // Default implementation for normal mode - do nothing
    }

    /**
     * Close resources
     *
     * @throws IOException if close fails
     */
    public void close() throws IOException {
        // Default implementation - do nothing
    }

    /**
     * Get an available host from the configured node URLs
     * This method implements round-robin selection with connection testing
     *
     * @return available host URL, or null if no host is available
     */
    protected String getAvailableHost() {
        java.util.List<String> hostList = sinkConfig.getNodeUrls();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = "http://" + hostList.get((int) (pos % hostList.size()));
            if (testConnection(host)) {
                return host;
            }
        }
        return null;
    }

    /**
     * Test connection to a host
     * Subclasses should implement this method based on their HTTP client
     */
    protected boolean testConnection(String host){
        return httpHelper.tryHttpConnection(host);
    }

    /**
     * Check if batch size is within limits
     * This method validates that the batch size doesn't exceed StarRocks limits
     *
     * @param batchMaxBytes the total bytes in the batch
     * @param batchMaxRows the number of rows in the batch
     * @throws StarRocksConnectorException if batch size exceeds limits
     */
    protected void checkBatchMaxBytes(long batchMaxBytes, long batchMaxRows) {
        long batchMaxBytesLimit;
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            Map<String, Object> props = sinkConfig.getStreamLoadProps();
            byte[] lineDelimiter =
                    StarRocksDelimiterParser.parse((String) props.get("row_delimiter"), "\n")
                            .getBytes(StandardCharsets.UTF_8);
            batchMaxBytesLimit = Integer.MAX_VALUE - batchMaxRows * lineDelimiter.length;
        } else if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            batchMaxBytesLimit = Integer.MAX_VALUE - (batchMaxRows == 0 ? 2 : batchMaxRows + 1);
        } else {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    "Failed to join rows data, unsupported `format` from stream load properties:");
        }

        if (batchMaxBytes > batchMaxBytesLimit) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    String.format(
                            "The batch_max_bytes[%d] of the data exceeds the maximum limit[%d], "
                                    + "please reset the batch_max_bytes.",
                            batchMaxBytes, batchMaxBytesLimit));
        }
    }
}
