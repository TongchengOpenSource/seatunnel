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

import java.util.Map;

public class FlussOptions {

    public static final String CONNECTOR_IDENTITY = "Fluss";

    public static final Option<String> BOOTSTRAP_SERVERS =
            Options.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Comma-separated list of Fluss bootstrap servers");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fluss database name");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fluss table name");

    public static final Option<Map<String, String>> FLUSS_CONFIG =
            Options.key("fluss.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Additional Fluss client configuration properties");

    public static final Option<Integer> CONNECTION_TIMEOUT_MS =
            Options.key("connection.timeout.ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Connection timeout in milliseconds");

    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request.timeout.ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Request timeout in milliseconds");

    public static final Option<Integer> RETRY_TIMES =
            Options.key("retry.times")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Number of retry attempts for failed operations");

    public static final Option<Long> RETRY_INTERVAL_MS =
            Options.key("retry.interval.ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Interval between retry attempts in milliseconds");
}
