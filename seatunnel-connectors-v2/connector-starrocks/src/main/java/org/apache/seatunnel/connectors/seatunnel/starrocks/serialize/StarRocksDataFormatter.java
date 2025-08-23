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

package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Data formatter for StarRocks stream load */
public class StarRocksDataFormatter {

    private final SinkConfig sinkConfig;
    private final ByteArrayOutputStream dataBuffer;
    private boolean isFirstRecord = true;

    public StarRocksDataFormatter(SinkConfig sinkConfig, ByteArrayOutputStream dataBuffer) {
        this.sinkConfig = sinkConfig;
        this.dataBuffer = dataBuffer;
    }

    /** Write record to buffer with proper formatting */
    public void writeRecord(byte[] record) throws IOException {
        if (!isFirstRecord) {
            writeDelimiter();
        } else {
            writePrefix();
            isFirstRecord = false;
        }

        dataBuffer.write(record);
    }

    /** Finalize the data buffer (e.g., close JSON array) */
    public void finalizeBuffer() throws IOException {
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            dataBuffer.write("]".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Reset the formatter state */
    public void reset() {
        isFirstRecord = true;
    }

    /** Write delimiter between records */
    private void writeDelimiter() throws IOException {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            Map<String, Object> props = sinkConfig.getStreamLoadProps();
            String rowDelimiter = (String) props.getOrDefault("row_delimiter", "\n");
            dataBuffer.write(rowDelimiter.getBytes(StandardCharsets.UTF_8));
        } else if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            dataBuffer.write(",".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Write prefix for the first record (e.g., open JSON array) */
    private void writePrefix() throws IOException {
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            dataBuffer.write("[".getBytes(StandardCharsets.UTF_8));
        }
    }
}
