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

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fluss.source.model.FlussTableBucket;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Slf4j
public class FlussSourceReader implements SourceReader<SeaTunnelRow, FlussSourceSplit> {

    private final Queue<FlussSourceSplit> pendingSplits;
    private final Context context;
    private final SourceConfig sourceConfig;
    private volatile boolean noMoreSplitsAssignment;

    private final Map<String, SeaTunnelRowType> tables;

    public FlussSourceReader(Context readerContext, SourceConfig sourceConfig) {
        this.pendingSplits = new LinkedList<>();
        this.context = readerContext;
        this.sourceConfig = sourceConfig;

        Map<String, SeaTunnelRowType> tables = new HashMap<>();
        sourceConfig
                .getTableConfigList()
                .forEach(
                        flussTableConfig ->
                                tables.put(
                                        flussTableConfig.getTable(),
                                        flussTableConfig
                                                .getCatalogTable()
                                                .getSeaTunnelRowType()));
        this.tables = tables;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                FlussSourceSplit split = pendingSplits.poll();
                read(split, output);
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())
                && noMoreSplitsAssignment
                && pendingSplits.isEmpty()) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded StarRocks source");
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<FlussSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<FlussSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
    }

    private void read(FlussSourceSplit split, Collector<SeaTunnelRow> output) {

        FlussTableBucket tableBucket = split.getBucket();
        String database = tableBucket.getDatabase();
        String tableName = tableBucket.getTable();
        TablePath tablePath = TablePath.of(database, tableName);
        Table table = connection.getTable(tablePath);
        LogScanner logScanner = table.newScan().createLogScanner();
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }
        long scanned = 0;
        Map<Integer, List<String>> rowsMap = new HashMap<>();

        while (true) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    InternalRow row = record.getRow();
                }
            }
            scanned += scanRecords.count();
        }
    }

    @Override
    public void open() throws Exception {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
