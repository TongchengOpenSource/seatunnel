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

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/** StarRocks transaction stream load implementation */
@Slf4j
public class StarRocksTransactionStreamLoad {

    private static final String BEGIN_TXN_URL_PATTERN = "http://%s/api/transaction/begin";
    private static final String LOAD_URL_PATTERN = "http://%s/api/transaction/load";
    private static final String PREPARE_URL_PATTERN = "http://%s/api/transaction/prepare";

    private final SinkConfig sinkConfig;
    private final String hostPort;
    private final String database;
    private final String table;
    private final String label;
    private final ByteArrayOutputStream dataBuffer;
    private final StarRocksHttpClient httpClient;
    private final StarRocksDataFormatter dataFormatter;

    private Long txnId;
    private boolean transactionStarted = false;

    public StarRocksTransactionStreamLoad(SinkConfig sinkConfig, String hostPort, String label) {
        this.sinkConfig = sinkConfig;
        this.hostPort = hostPort;
        this.database = sinkConfig.getDatabase();
        this.table = sinkConfig.getTable();
        this.label = label;
        this.dataBuffer = new ByteArrayOutputStream();
        this.httpClient = new StarRocksHttpClient(sinkConfig);
        this.dataFormatter = new StarRocksDataFormatter(sinkConfig, dataBuffer);
    }

    public void beginTransaction() throws IOException {
        if (transactionStarted) {
            return;
        }

        String beginUrl = String.format(BEGIN_TXN_URL_PATTERN, hostPort);
        HttpPost httpPost = new HttpPost(beginUrl);

        httpClient.setCommonHeaders(httpPost, label, database, table);
        httpClient.setEmptyEntity(httpPost);

        StarRocksHttpClient.StarRocksHttpResponse response =
                httpClient.executeRequest(httpPost, "Begin transaction");

        httpClient.validateResponse(response, "Begin transaction");

        this.txnId = response.getTxnId();
        this.transactionStarted = true;
        log.info("Successfully started transaction for label: {}, txnId: {}", label, txnId);
    }

    /** Write record to buffer */
    public void writeRecord(byte[] record) throws IOException {
        if (!transactionStarted) {
            beginTransaction();
        }

        dataFormatter.writeRecord(record);
    }

    /** Load data to StarRocks */
    public void loadData() throws IOException {
        if (!transactionStarted) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, "Transaction not started");
        }

        if (dataBuffer.size() == 0) {
            return;
        }

        // Finalize data buffer
        dataFormatter.finalizeBuffer();

        String loadUrl = String.format(LOAD_URL_PATTERN, hostPort);
        HttpPut httpPut = new HttpPut(loadUrl);

        httpClient.setCommonHeaders(httpPut, label, database, table);

        // Set stream load properties
        Map<String, Object> streamLoadProps = sinkConfig.getStreamLoadProps();
        for (Map.Entry<String, Object> entry : streamLoadProps.entrySet()) {
            httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
        }

        httpPut.setEntity(new ByteArrayEntity(dataBuffer.toByteArray()));

        StarRocksHttpClient.StarRocksHttpResponse response =
                httpClient.executeRequest(httpPut, "Load data");

        httpClient.validateResponse(response, "Load data");

        log.info("Successfully loaded data for label: {}, txnId: {}", label, txnId);

        // Clear buffer after successful load
        dataBuffer.reset();
        dataFormatter.reset();
    }

    /** Prepare commit transaction */
    public void prepareCommit() throws IOException {
        if (!transactionStarted) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, "Transaction not started");
        }

        String prepareUrl = String.format(PREPARE_URL_PATTERN, hostPort);
        HttpPost httpPost = new HttpPost(prepareUrl);

        httpClient.setCommonHeaders(httpPost, label, database);
        httpClient.setEmptyEntity(httpPost);

        StarRocksHttpClient.StarRocksHttpResponse response =
                httpClient.executeRequest(httpPost, "Prepare commit");

        httpClient.validateResponse(response, "Prepare commit");

        log.info("Successfully prepared transaction for label: {}, txnId: {}", label, txnId);
    }

    /** Get commit info for committer */
    public StarRocksCommitInfo getCommitInfo() {
        if (!transactionStarted || txnId == null) {
            return null;
        }
        return new StarRocksCommitInfo(hostPort, database, label, txnId);
    }

    /** Close resources */
    public void close() throws IOException {
        if (dataBuffer != null) {
            dataBuffer.close();
        }
    }

    public boolean isTransactionStarted() {
        return transactionStarted;
    }

    public Long getTxnId() {
        return txnId;
    }

    public String getLabel() {
        return label;
    }
}
