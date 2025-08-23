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

import org.apache.seatunnel.api.table.catalog.TableSchema;
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

/** StarRocks transaction stream load visitor for 2PC mode */
@Slf4j
public class StarRocksTransactionStreamLoadVisitor extends AbstractStreamLoadVisitor {

    private static final String BEGIN_TXN_URL_PATTERN = "http://%s/api/transaction/begin";
    private static final String LOAD_URL_PATTERN = "http://%s/api/transaction/load";
    private static final String PREPARE_URL_PATTERN = "http://%s/api/transaction/prepare";

    private String hostPort;
    private String database;
    private String table;
    private String label;
    private ByteArrayOutputStream dataBuffer;
    private StarRocksHttpClient httpClient;
    private StarRocksDataFormatter dataFormatter;

    private Long txnId;
    private boolean transactionStarted = false;

    public StarRocksTransactionStreamLoadVisitor(SinkConfig sinkConfig, TableSchema tableSchema) {
        super(sinkConfig, tableSchema);
        this.database = sinkConfig.getDatabase();
        this.table = sinkConfig.getTable();
        this.httpClient = new StarRocksHttpClient(sinkConfig);
    }

    @Override
    public boolean doStreamLoad(StarRocksFlushTuple flushData) throws IOException {
        if (hostPort == null) {
            hostPort = getAvailableHost();
            if (hostPort == null) {
                throw new IOException("No available host for StarRocks connection");
            }
            this.label = flushData.getLabel();
            this.dataBuffer = new ByteArrayOutputStream();
            this.dataFormatter = new StarRocksDataFormatter(sinkConfig, dataBuffer);
        }
        for (byte[] record : flushData.getRows()) {
            writeRecord(record);
        }
        loadData();

        return true;
    }

    private void beginTransaction() throws IOException {
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

    private void writeRecord(byte[] record) throws IOException {
        if (!transactionStarted) {
            beginTransaction();
        }

        dataFormatter.writeRecord(record);
    }

    private void loadData() throws IOException {
        if (!transactionStarted) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, "Transaction not started");
        }

        if (dataBuffer.size() == 0) {
            return;
        }

        dataFormatter.finalizeBuffer();
        String loadUrl = String.format(LOAD_URL_PATTERN, hostPort);
        HttpPut httpPut = new HttpPut(loadUrl);

        httpClient.setCommonHeaders(httpPut, label, database, table);

        Map<String, Object> streamLoadProps = sinkConfig.getStreamLoadProps();
        for (Map.Entry<String, Object> entry : streamLoadProps.entrySet()) {
            httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
        }

        httpPut.setEntity(new ByteArrayEntity(dataBuffer.toByteArray()));

        StarRocksHttpClient.StarRocksHttpResponse response =
                httpClient.executeRequest(httpPut, "Load data");

        httpClient.validateResponse(response, "Load data");

        log.info("Successfully loaded data for label: {}, txnId: {}", label, txnId);

        dataBuffer.reset();
        dataFormatter.reset();
    }

    @Override
    public StarRocksCommitInfo prepareCommit() throws IOException {
        if (transactionStarted && txnId != null) {
            prepareCommitTransaction();
            StarRocksCommitInfo commitInfo = getCommitInfo();
            resetTransaction();
            return commitInfo;
        }
        return null;
    }

    private void prepareCommitTransaction() throws IOException {
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

    private StarRocksCommitInfo getCommitInfo() {
        if (!transactionStarted || txnId == null) {
            return null;
        }
        return new StarRocksCommitInfo(hostPort, label, database, txnId);
    }

    /** Reset transaction state for next checkpoint */
    private void resetTransaction() {
        this.hostPort = null;
        this.label = null;
        this.txnId = null;
        this.transactionStarted = false;
        if (dataBuffer != null) {
            try {
                dataBuffer.close();
            } catch (IOException e) {
                log.warn("Error closing data buffer", e);
            }
            dataBuffer = null;
        }
        dataFormatter = null;
    }

    @Override
    public void abortTransaction() {
        if (transactionStarted) {
            try {
                resetTransaction();
            } catch (Exception e) {
                log.warn("Error during transaction abort", e);
            }
        }
    }

    @Override
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
