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

import java.time.Duration;
import java.util.Map;

/**
 * Core Fluss connector options following the official Fluss design pattern.
 *
 * <p>This class only defines connector-specific options. Table and client options are handled
 * through the fluss.config map to avoid duplication and maintain compatibility with Fluss core
 * configurations.
 */
public class FlussOptions {

    public static final String CONNECTOR_IDENTITY = "Fluss";

    // ----------------------------------------------------------------------------------------
    // Core connection options
    // ----------------------------------------------------------------------------------------

    public static final Option<String> BOOTSTRAP_SERVERS =
            Options.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A list of host/port pairs to use for establishing the initial connection to the Fluss cluster. "
                                    + "The list should be in the form host1:port1,host2:port2,....");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fluss database name");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("Fluss table name");

    // ----------------------------------------------------------------------------------------
    // Bucket distribution options
    // ----------------------------------------------------------------------------------------

    public static final Option<Integer> BUCKET_NUMBER =
            Options.key("bucket.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of buckets of a Fluss table.");

    public static final Option<String> BUCKET_KEY =
            Options.key("bucket.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specific the distribution policy of the Fluss table. "
                                    + "Data will be distributed to each bucket according to the hash value of bucket-key "
                                    + "(It must be a subset of the primary keys excluding partition keys of the primary key table). "
                                    + "If you specify multiple fields, delimiter is ','. "
                                    + "If the table has a primary key and a bucket key is not specified, the bucket key will be used as primary key(excluding the partition key). "
                                    + "If the table has no primary key and the bucket key is not specified, "
                                    + "the data will be distributed to each bucket randomly.");

    // ----------------------------------------------------------------------------------------
    // Scan specific options
    // ----------------------------------------------------------------------------------------

    public static final Option<Duration> SCAN_PARTITION_DISCOVERY_INTERVAL =
            Options.key("scan.partition.discovery.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The time interval for the Fluss source to discover "
                                    + "the new partitions for partitioned table while scanning. "
                                    + "A non-positive value disables the partition discovery.");

    // ----------------------------------------------------------------------------------------
    // Lookup specific options
    // ----------------------------------------------------------------------------------------

    public static final Option<Boolean> LOOKUP_ASYNC =
            Options.key("lookup.async")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to set async lookup. Default is true.");

    // ----------------------------------------------------------------------------------------
    // Sink specific options
    // ----------------------------------------------------------------------------------------

    public static final Option<Boolean> SINK_IGNORE_DELETE =
            Options.key("sink.ignore-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore retract（-U/-D) record.");

    public static final Option<Boolean> SINK_BUCKET_SHUFFLE =
            Options.key("sink.bucket-shuffle")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to shuffle by bucket id before write to sink. Shuffling the data with the same "
                                    + "bucket id to be processed by the same task can improve the efficiency of client "
                                    + "processing and reduce resource consumption. For Log Table, bucket shuffle will "
                                    + "only take effect when the 'bucket.key' is defined. For Primary Key table, it is enabled by default.");

    // ----------------------------------------------------------------------------------------
    // Additional configuration
    // ----------------------------------------------------------------------------------------

    public static final Option<Map<String, String>> FLUSS_CONFIG =
            Options.key("fluss.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Additional Fluss configuration properties. "
                                    + "This can include table options, client options, and other Fluss-specific configurations. "
                                    + "For example: {'client.id': 'my-client', 'table.log.retention.ms': '86400000'}");
}
