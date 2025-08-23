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

package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.transaction;

import java.nio.charset.StandardCharsets;

public class StarRocksTransactionCSVSerializer implements  StarRocksTransactionISerializer {

    private static final byte[] EMPTY_DELIMITER = new byte[0];
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private final byte[] delimiter;

    public StarRocksTransactionCSVSerializer() {
        this(DEFAULT_LINE_DELIMITER);
    }

    public StarRocksTransactionCSVSerializer(String rowDelimiter) {
        if (rowDelimiter == null) {
            throw new IllegalArgumentException("row delimiter can not be null");
        }

        this.delimiter = rowDelimiter.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] first() {
        return EMPTY_DELIMITER;
    }

    @Override
    public byte[] delimiter() {
        return delimiter;
    }

    @Override
    public byte[] end() {
        // For transaction stream load, need to append the row delimiter to the end of each
        // load, and separate the data in multiple loads. For example, there are 3 columns,
        // column delimiter ",", row delimiter "\n". There are 2 rows (1, 2, 3), (4, 5, 6),
        // and send them in two loads. If not append the end delimiter, the data received by
        // StarRocks will be "1,2,34,5,6", the two rows are not separated by row delimiter, and
        // StarRocks will parse it as one line with 5 columns. After appending the end
        // delimiter,
        // it will be "1,2,3\n4,5,6", which will be parsed correctly
        return delimiter;
    }
}
