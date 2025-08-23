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

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksHttpClient;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksTransactionStreamLoadVisitor;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import org.apache.http.client.methods.HttpPost;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** StarRocks committer for transaction stream load */
@Slf4j
public class StarRocksCommitter implements SinkCommitter<StarRocksCommitInfo> {

    private final SinkConfig sinkConfig;
    private final StarRocksTransactionStreamLoadVisitor transactionStreamLoadVisitor;

    public StarRocksCommitter(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.transactionStreamLoadVisitor = new StarRocksTransactionStreamLoadVisitor(sinkConfig, null);
    }

    @Override
    public List<StarRocksCommitInfo> commit(List<StarRocksCommitInfo> commitInfos)
            throws IOException {
        for (StarRocksCommitInfo commitInfo : commitInfos) {
            transactionStreamLoadVisitor.commit(commitInfo);
        }
        return Collections.emptyList();
    }

    @Override
    public void abort(List<StarRocksCommitInfo> commitInfos) throws IOException {
        for (StarRocksCommitInfo commitInfo : commitInfos) {
            transactionStreamLoadVisitor.rollback(commitInfo);
        }
    }

}
