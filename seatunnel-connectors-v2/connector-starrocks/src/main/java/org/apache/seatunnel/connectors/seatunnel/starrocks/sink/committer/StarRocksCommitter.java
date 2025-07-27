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
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import org.apache.http.client.methods.HttpPost;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** StarRocks committer for transaction stream load */
@Slf4j
public class StarRocksCommitter implements SinkCommitter<StarRocksCommitInfo> {

    private static final String COMMIT_PATTERN = "http://%s/api/transaction/commit";
    private static final String ROLLBACK_PATTERN = "http://%s/api/transaction/rollback";
    private static final int MAX_RETRY = 3;

    private final SinkConfig sinkConfig;
    private final StarRocksHttpClient httpClient;

    public StarRocksCommitter(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.httpClient = new StarRocksHttpClient(sinkConfig);
    }

    @Override
    public List<StarRocksCommitInfo> commit(List<StarRocksCommitInfo> commitInfos)
            throws IOException {
        for (StarRocksCommitInfo commitInfo : commitInfos) {
            commitTransaction(commitInfo);
        }
        return Collections.emptyList();
    }

    @Override
    public void abort(List<StarRocksCommitInfo> commitInfos) throws IOException {
        for (StarRocksCommitInfo commitInfo : commitInfos) {
            rollbackTransaction(commitInfo);
        }
    }

    private void commitTransaction(StarRocksCommitInfo commitInfo) throws IOException {
        String commitUrl = String.format(COMMIT_PATTERN, commitInfo.getHostPort());
        HttpPost httpPost = new HttpPost(commitUrl);

        httpClient.setCommonHeaders(httpPost, commitInfo.getLabel(), commitInfo.getDb());
        httpClient.setEmptyEntity(httpPost);

        StarRocksHttpClient.StarRocksHttpResponse response =
                httpClient.executeRequestWithRetry(httpPost, "Commit transaction", MAX_RETRY);

        httpClient.validateResponse(response, "Commit transaction");

        log.info(
                "Successfully committed transaction for label: {}, txnId: {}",
                commitInfo.getLabel(),
                commitInfo.getTxnId());
    }

    private void rollbackTransaction(StarRocksCommitInfo commitInfo) throws IOException {
        String rollbackUrl = String.format(ROLLBACK_PATTERN, commitInfo.getHostPort());
        HttpPost httpPost = new HttpPost(rollbackUrl);

        httpClient.setCommonHeaders(httpPost, commitInfo.getLabel(), commitInfo.getDb());
        httpClient.setEmptyEntity(httpPost);

        try {
            StarRocksHttpClient.StarRocksHttpResponse response =
                    httpClient.executeRequest(httpPost, "Rollback transaction");

            httpClient.validateResponseWithWarning(
                    response, "rollback transaction", commitInfo.getLabel(), commitInfo.getTxnId());
        } catch (Exception e) {
            log.warn(
                    "Failed to rollback transaction for label: {}, txnId: {}",
                    commitInfo.getLabel(),
                    commitInfo.getTxnId(),
                    e);
        }
    }
}
