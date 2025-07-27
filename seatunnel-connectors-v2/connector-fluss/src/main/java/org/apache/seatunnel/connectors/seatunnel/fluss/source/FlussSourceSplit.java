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

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlussSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private String splitId;
    private String database;
    private String table;
    private int bucketId;
    private long startOffset;
    private long endOffset;
    private boolean isFinished;

    public FlussSourceSplit(String splitId, String database, String table, int bucketId) {
        this.splitId = splitId;
        this.database = database;
        this.table = table;
        this.bucketId = bucketId;
        this.startOffset = -1L; // Start from beginning
        this.endOffset = -1L; // Read to end
        this.isFinished = false;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getFullTableName() {
        return database + "." + table;
    }

    public void updateOffset(long offset) {
        this.startOffset = offset;
    }

    public void markAsFinished() {
        this.isFinished = true;
    }

    @Override
    public String toString() {
        return "FlussSourceSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", bucketId="
                + bucketId
                + ", startOffset="
                + startOffset
                + ", endOffset="
                + endOffset
                + ", isFinished="
                + isFinished
                + '}';
    }
}
