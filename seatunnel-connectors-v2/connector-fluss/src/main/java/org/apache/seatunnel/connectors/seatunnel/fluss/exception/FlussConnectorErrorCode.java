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

package org.apache.seatunnel.connectors.seatunnel.fluss.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum FlussConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECTION_FAILED("FLUSS-01", "Failed to connect to Fluss cluster"),
    TABLE_NOT_FOUND("FLUSS-02", "Fluss table not found"),
    DATABASE_NOT_FOUND("FLUSS-03", "Fluss database not found"),
    INVALID_CONFIGURATION("FLUSS-04", "Invalid Fluss configuration"),
    READ_DATA_FAILED("FLUSS-05", "Failed to read data from Fluss"),
    WRITE_DATA_FAILED("FLUSS-06", "Failed to write data to Fluss"),
    SCHEMA_MISMATCH("FLUSS-07", "Schema mismatch between SeaTunnel and Fluss"),
    TRANSACTION_FAILED("FLUSS-08", "Fluss transaction failed"),
    SERIALIZATION_FAILED("FLUSS-09", "Failed to serialize/deserialize data"),
    TIMEOUT_ERROR("FLUSS-10", "Operation timeout"),
    AUTHENTICATION_FAILED("FLUSS-11", "Authentication failed"),
    AUTHORIZATION_FAILED("FLUSS-12", "Authorization failed"),
    UNSUPPORTED_OPERATION("FLUSS-13", "Unsupported operation"),
    RESOURCE_NOT_AVAILABLE("FLUSS-14", "Resource not available"),
    INTERNAL_ERROR("FLUSS-15", "Internal Fluss connector error");

    private final String code;
    private final String description;

    FlussConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
