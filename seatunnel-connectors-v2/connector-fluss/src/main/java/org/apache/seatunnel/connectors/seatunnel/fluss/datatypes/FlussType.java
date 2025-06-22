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
package org.apache.seatunnel.connectors.seatunnel.fluss.datatypes;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class FlussType {
    public static final String NULL = "NULL";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String DATE = "DATE";
    public static final String SR_DATETIME = "DATETIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String TIMESTAMP_LTZ = "TIMESTAMP_LTZ";
    public static final String BINARY = "BINARY";
    public static final String BYTES = "BYTES";
    /**
     * A fixed-length character string where n is the number of code points. n must have a value between 1 and Integer.MAX_VALUE (both inclusive).
     */
    public static final String CHAR = "CHAR";
    public static final String STRING = "STRING";

    private String type;
}
