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

package org.apache.seatunnel.connectors.seatunnel.fluss.catalog;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussOptions;

/**
 * Options for Fluss catalog, extending the core Fluss options.
 * 
 * <p>This class defines catalog-specific options that are used for catalog operations
 * such as listing databases, tables, and managing metadata.
 */
public class FlussCatalogOptions extends FlussOptions {

    public static final Option<String> DEFAULT_DATABASE =
            Options.key("default-database")
                    .stringType()
                    .defaultValue("fluss")
                    .withDescription(
                            "Default database name used when none is specified in catalog operations");

    public static final Option<Boolean> CASE_SENSITIVE =
            Options.key("case-sensitive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the catalog is case-sensitive for database and table names");

    public static final Option<Integer> CONNECTION_POOL_SIZE =
            Options.key("connection.pool.size")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Maximum number of connections in the connection pool for catalog operations");

    public static final Option<Long> METADATA_CACHE_TTL_MS =
            Options.key("metadata.cache.ttl.ms")
                    .longType()
                    .defaultValue(300000L) // 5 minutes
                    .withDescription(
                            "Time-to-live for metadata cache in milliseconds. "
                                    + "Set to 0 to disable caching");

    public static final Option<Integer> METADATA_CACHE_SIZE =
            Options.key("metadata.cache.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum number of entries in the metadata cache");
}
