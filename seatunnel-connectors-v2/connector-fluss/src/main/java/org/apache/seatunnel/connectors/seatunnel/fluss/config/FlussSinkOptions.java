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

package org.apache.seatunnel.connectors.seatunnel.fluss.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class FlussSinkOptions extends FlussOptions {

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of records to batch before writing to Fluss");

    public static final Option<Long> BATCH_TIMEOUT_MS =
            Options.key("batch.timeout.ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("Maximum time to wait before flushing a batch in milliseconds");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max.retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries for failed writes");

    public static final Option<String> WRITE_MODE =
            Options.key("write.mode")
                    .stringType()
                    .defaultValue("append")
                    .withDescription("Write mode: append, upsert");

    public static final Option<Boolean> ENABLE_TRANSACTION =
            Options.key("enable.transaction")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable transactional writes");

    public static final Option<String> TRANSACTION_TIMEOUT =
            Options.key("transaction.timeout")
                    .stringType()
                    .defaultValue("15min")
                    .withDescription("Transaction timeout duration");

    public static final Option<Boolean> ENABLE_EXACTLY_ONCE =
            Options.key("enable.exactly.once")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable exactly-once semantics");

    public static final Option<String> TRANSACTIONAL_ID_PREFIX =
            Options.key("transactional.id.prefix")
                    .stringType()
                    .defaultValue("seatunnel-fluss")
                    .withDescription("Prefix for transactional IDs");
}
