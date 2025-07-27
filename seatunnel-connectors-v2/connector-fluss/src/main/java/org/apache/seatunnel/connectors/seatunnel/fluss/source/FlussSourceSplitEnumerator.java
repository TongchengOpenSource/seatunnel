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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class FlussSourceSplitEnumerator
        implements SourceSplitEnumerator<FlussSourceSplit, FlussSourceState> {

    private final SourceSplitEnumerator.Context<FlussSourceSplit> context;
    private final FlussSourceConfig sourceConfig;
    private final Set<FlussSourceSplit> pendingSplits;
    private final Set<String> assignedSplits;

    public FlussSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FlussSourceSplit> context,
            FlussSourceConfig sourceConfig) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.pendingSplits = new HashSet<>();
        this.assignedSplits = new HashSet<>();
    }

    public FlussSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FlussSourceSplit> context,
            FlussSourceConfig sourceConfig,
            FlussSourceState sourceState) {
        this(context, sourceConfig);
        if (sourceState != null) {
            this.assignedSplits.addAll(sourceState.getAssignedSplits());
        }
    }

    @Override
    public void open() {
        try {
            discoverSplits();
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.INTERNAL_ERROR,
                    "Failed to discover Fluss table splits",
                    e);
        }
    }

    @Override
    public void run() throws Exception {
        // Assign pending splits to available readers
        assignSplits();
    }

    @Override
    public void close() throws IOException {
        // Clean up resources
        log.info("Closing Fluss source split enumerator");
    }

    @Override
    public void addSplitsBack(List<FlussSourceSplit> splits, int subtaskId) {
        log.info("Adding back {} splits from subtask {}", splits.size(), subtaskId);
        for (FlussSourceSplit split : splits) {
            assignedSplits.remove(split.splitId());
            pendingSplits.add(split);
        }
        assignSplits();
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        log.debug("Handling split request from subtask {}", subtaskId);
        assignSplits();
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Registering reader for subtask {}", subtaskId);
        assignSplits();
    }

    @Override
    public FlussSourceState snapshotState(long checkpointId) throws Exception {
        return new FlussSourceState(new ArrayList<>(assignedSplits));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Handle checkpoint completion if needed
    }

    private void discoverSplits() {
        // For now, create splits based on parallelism
        // In a real implementation, this would query Fluss to get actual bucket information
        int parallelism = sourceConfig.getScanParallelism();

        for (int i = 0; i < parallelism; i++) {
            String splitId =
                    String.format(
                            "%s-%s-bucket-%d",
                            sourceConfig.getDatabase(), sourceConfig.getTable(), i);

            FlussSourceSplit split =
                    new FlussSourceSplit(
                            splitId, sourceConfig.getDatabase(), sourceConfig.getTable(), i);

            pendingSplits.add(split);
        }

        log.info(
                "Discovered {} splits for table {}",
                pendingSplits.size(),
                sourceConfig.getFullTableName());
    }

    private void assignSplits() {
        if (pendingSplits.isEmpty()) {
            return;
        }

        Map<Integer, List<FlussSourceSplit>> assignment = new HashMap<>();

        for (int readerId : context.registeredReaders()) {
            assignment.put(readerId, new ArrayList<>());
        }

        // Simple round-robin assignment
        List<FlussSourceSplit> splitsToAssign = new ArrayList<>(pendingSplits);
        int readerIndex = 0;
        List<Integer> readers = new ArrayList<>(context.registeredReaders());

        for (FlussSourceSplit split : splitsToAssign) {
            if (!readers.isEmpty()) {
                int readerId = readers.get(readerIndex % readers.size());
                assignment.get(readerId).add(split);
                assignedSplits.add(split.splitId());
                readerIndex++;
            }
        }

        // Assign splits to readers
        for (Map.Entry<Integer, List<FlussSourceSplit>> entry : assignment.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                context.assignSplit(entry.getKey(), entry.getValue());
                log.info(
                        "Assigned {} splits to reader {}", entry.getValue().size(), entry.getKey());
            }
        }

        // Remove assigned splits from pending
        pendingSplits.removeAll(splitsToAssign);
    }
}
