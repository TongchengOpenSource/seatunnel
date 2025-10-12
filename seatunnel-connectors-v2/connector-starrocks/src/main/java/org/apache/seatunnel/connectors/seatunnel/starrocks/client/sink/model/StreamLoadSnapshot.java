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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.model;

import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.TableRegion;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Setter
@Getter
public class StreamLoadSnapshot implements Serializable {

    private String id;
    private List<Transaction> transactions;
    private long timestamp;

    public boolean isFinish(String database, String label) {
        for (Transaction transaction : transactions) {
            if (transaction.getDatabase().equals(database)
                    && transaction.getLabel().equals(label)) {
                return transaction.isFinish();
            }
        }
        return false;
    }

    @Setter
    @Getter
    public static class Transaction implements Serializable {
        private String database;
        private String table;
        private String label;
        private boolean finish;

        public Transaction(String database, String table, String label) {
            this.database = database;
            this.table = table;
            this.label = label;
            this.finish = false;
        }

        @Override
        public String toString() {
            return "Transaction{"
                    + "database='"
                    + database
                    + '\''
                    + ", table='"
                    + table
                    + '\''
                    + ", label='"
                    + label
                    + '\''
                    + ", finish="
                    + finish
                    + '}';
        }
    }

    public static StreamLoadSnapshot snapshot(Iterable<TableRegion> regions) {

        List<Transaction> transactions =
                StreamSupport.stream(regions.spliterator(), false)
                        .filter(region -> region.getLabel() != null)
                        .map(
                                region ->
                                        new Transaction(
                                                region.getDatabase(),
                                                region.getTable(),
                                                region.getLabel()))
                        .collect(Collectors.toList());

        StreamLoadSnapshot snapshot = new StreamLoadSnapshot();
        snapshot.setId(UUID.randomUUID().toString());
        snapshot.setTimestamp(System.currentTimeMillis());
        snapshot.setTransactions(transactions);

        return snapshot;
    }
}
