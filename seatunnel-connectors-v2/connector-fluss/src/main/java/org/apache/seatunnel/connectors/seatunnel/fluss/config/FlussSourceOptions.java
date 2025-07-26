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

public class FlussSourceOptions extends FlussOptions {

    public static final Option<String> SCAN_STARTUP_MODE =
            Options.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("earliest")
                    .withDescription("Startup mode for Fluss source: earliest, latest, timestamp");

    public static final Option<Long> SCAN_STARTUP_TIMESTAMP =
            Options.key("scan.startup.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Startup timestamp for timestamp mode (milliseconds since epoch)");

    public static final Option<Integer> SCAN_PARALLELISM =
            Options.key("scan.parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Parallelism for scanning Fluss table");

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of records to fetch in each batch");

    public static final Option<Long> POLL_TIMEOUT_MS =
            Options.key("poll.timeout.ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("Timeout for polling records in milliseconds");

    public static final Option<Boolean> ENABLE_CHANGELOG =
            Options.key("enable.changelog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable changelog mode for streaming reads");

    public static final Option<String> CONSUMER_GROUP =
            Options.key("consumer.group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Consumer group ID for Fluss source");
}
