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

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/** Common HTTP client for StarRocks transaction operations */
@Slf4j
public class StarRocksHttpClient {

    private static final String SUCCESS_STATUS = "OK";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SinkConfig sinkConfig;
    private final String authHeader;

    public StarRocksHttpClient(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.authHeader =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (sinkConfig.getUsername() + ":" + sinkConfig.getPassword())
                                                .getBytes(StandardCharsets.UTF_8));
    }

    /** Set common headers for StarRocks transaction requests */
    public void setCommonHeaders(HttpRequestBase request, String label, String database) {
        request.setHeader("Authorization", authHeader);
        request.setHeader("Content-Type", "application/json");
        request.setHeader("label", label);
        request.setHeader("db", database);
        request.setHeader("Expect", "100-continue");
    }

    /** Set common headers for StarRocks transaction requests with table */
    public void setCommonHeaders(
            HttpRequestBase request, String label, String database, String table) {
        setCommonHeaders(request, label, database);
        request.setHeader("table", table);
    }

    /** Set empty entity for POST requests */
    public void setEmptyEntity(HttpRequestBase request) {
        if (request instanceof org.apache.http.client.methods.HttpEntityEnclosingRequestBase) {
            ((org.apache.http.client.methods.HttpEntityEnclosingRequestBase) request)
                    .setEntity(new StringEntity("", StandardCharsets.UTF_8));
        }
    }

    /** Execute HTTP request and handle response */
    public StarRocksHttpResponse executeRequest(HttpRequestBase request, String operation)
            throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(request)) {

            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode != HttpStatus.SC_OK) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                        operation + " failed with status code: " + statusCode);
            }

            HttpEntity entity = response.getEntity();
            if (entity == null) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                        operation + " failed: empty response");
            }

            String result = EntityUtils.toString(entity);
            Map<String, Object> resultMap =
                    OBJECT_MAPPER.readValue(
                            result, new TypeReference<HashMap<String, Object>>() {});

            return new StarRocksHttpResponse(statusCode, resultMap);
        }
    }

    /** Execute HTTP request with retry mechanism */
    public StarRocksHttpResponse executeRequestWithRetry(
            HttpRequestBase request, String operation, int maxRetries) throws IOException {
        Exception lastException = null;

        for (int retry = 0; retry <= maxRetries; retry++) {
            try {
                return executeRequest(request, operation);
            } catch (Exception e) {
                lastException = e;
                if (retry >= maxRetries) {
                    break;
                }

                log.warn("{} failed, retry {}/{}", operation, retry + 1, maxRetries, e);
                try {
                    Thread.sleep(1000 * (retry + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, ie);
                }
            }
        }

        throw new StarRocksConnectorException(
                StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                operation + " failed after " + maxRetries + " retries",
                lastException);
    }

    /** Validate response status */
    public void validateResponse(StarRocksHttpResponse response, String operation) {
        if (!SUCCESS_STATUS.equals(response.getResultMap().get("Status"))) {
            String message = (String) response.getResultMap().get("Message");
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    operation + " failed: " + message);
        }
    }

    /** Validate response status with warning on failure */
    public void validateResponseWithWarning(
            StarRocksHttpResponse response, String operation, String label, Long txnId) {
        if (!SUCCESS_STATUS.equals(response.getResultMap().get("Status"))) {
            String message = (String) response.getResultMap().get("Message");
            log.warn(
                    "Failed to {} for label: {}, txnId: {}, message: {}",
                    operation,
                    label,
                    txnId,
                    message);
        } else {
            log.info("Successfully {} for label: {}, txnId: {}", operation, label, txnId);
        }
    }

    /** HTTP response wrapper */
    public static class StarRocksHttpResponse {
        private final int statusCode;
        private final Map<String, Object> resultMap;

        public StarRocksHttpResponse(int statusCode, Map<String, Object> resultMap) {
            this.statusCode = statusCode;
            this.resultMap = resultMap;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public Map<String, Object> getResultMap() {
            return resultMap;
        }

        public boolean isSuccess() {
            return SUCCESS_STATUS.equals(resultMap.get("Status"));
        }

        public String getMessage() {
            return (String) resultMap.get("Message");
        }

        public Long getTxnId() {
            Object txnId = resultMap.get("TxnId");
            return txnId != null ? ((Number) txnId).longValue() : null;
        }
    }
}
