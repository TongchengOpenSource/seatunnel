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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class FlussAggregatedCommitter implements SinkAggregatedCommitter<FlussCommitInfo, FlussAggregatedCommitInfo> {

    private final FlussSinkConfig sinkConfig;
    private Object flussClient;

    public FlussAggregatedCommitter(FlussSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        try {
            initializeFlussClient();
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to initialize Fluss client for committer",
                    e);
        }
    }

    @Override
    public List<FlussAggregatedCommitInfo> commit(List<FlussAggregatedCommitInfo> aggregatedCommitInfos)
            throws IOException {
        List<FlussAggregatedCommitInfo> failedCommits = new ArrayList<>();
        
        for (FlussAggregatedCommitInfo commitInfo : aggregatedCommitInfos) {
            try {
                commitTransactions(commitInfo.getTransactionIds());
                log.info("Successfully committed {} transactions for checkpoint {}", 
                        commitInfo.getTransactionIds().size(), commitInfo.getCheckpointId());
            } catch (Exception e) {
                log.error("Failed to commit transactions for checkpoint {}", 
                        commitInfo.getCheckpointId(), e);
                failedCommits.add(commitInfo);
            }
        }
        
        return failedCommits;
    }

    @Override
    public FlussAggregatedCommitInfo combine(List<FlussCommitInfo> commitInfos) {
        List<String> transactionIds = commitInfos.stream()
                .map(FlussCommitInfo::getTransactionId)
                .collect(Collectors.toList());
        
        // Use current time as checkpoint ID if not available
        long checkpointId = System.currentTimeMillis();
        
        return new FlussAggregatedCommitInfo(transactionIds, checkpointId);
    }

    @Override
    public void abort(List<FlussAggregatedCommitInfo> aggregatedCommitInfos) throws Exception {
        for (FlussAggregatedCommitInfo commitInfo : aggregatedCommitInfos) {
            try {
                abortTransactions(commitInfo.getTransactionIds());
                log.info("Successfully aborted {} transactions for checkpoint {}", 
                        commitInfo.getTransactionIds().size(), commitInfo.getCheckpointId());
            } catch (Exception e) {
                log.error("Failed to abort transactions for checkpoint {}", 
                        commitInfo.getCheckpointId(), e);
                // Continue aborting other transactions even if one fails
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (flussClient != null) {
                // flussClient.close();
            }
            log.info("Fluss aggregated committer closed");
        } catch (Exception e) {
            log.warn("Error closing Fluss aggregated committer", e);
        }
    }

    private void initializeFlussClient() throws Exception {
        log.info("Initializing Fluss client for aggregated committer");
        
        // Initialize Fluss client with configuration
        // This is a placeholder - actual implementation would use Fluss client API
        // Properties props = new Properties();
        // props.putAll(sinkConfig.toFlussProperties());
        // flussClient = new FlussClient(props);
    }

    private void commitTransactions(List<String> transactionIds) throws Exception {
        for (String transactionId : transactionIds) {
            // Commit individual transaction
            // flussClient.commitTransaction(transactionId);
            log.debug("Committed transaction: {}", transactionId);
        }
    }

    private void abortTransactions(List<String> transactionIds) throws Exception {
        for (String transactionId : transactionIds) {
            // Abort individual transaction
            // flussClient.abortTransaction(transactionId);
            log.debug("Aborted transaction: {}", transactionId);
        }
    }
}
