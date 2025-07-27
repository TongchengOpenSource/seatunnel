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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer;

import java.io.Serializable;

/** StarRocks commit info for transaction stream load */
public class StarRocksCommitInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String hostPort;
    private String db;
    private String table;
    private String label;
    private long txnId;

    public StarRocksCommitInfo() {}

    public StarRocksCommitInfo(String hostPort, String db, String table, String label, long txnId) {
        this.hostPort = hostPort;
        this.db = db;
        this.table = table;
        this.label = label;
        this.txnId = txnId;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    @Override
    public String toString() {
        return "StarRocksCommitInfo{"
                + "hostPort='"
                + hostPort
                + '\''
                + ", db='"
                + db
                + '\''
                + ", table='"
                + table
                + '\''
                + ", label='"
                + label
                + '\''
                + ", txnId="
                + txnId
                + '}';
    }
}
