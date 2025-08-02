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

/**
 * Options for Fluss catalog, following the official Fluss Flink connector design.
 *
 * <p>This class defines catalog-specific options that are used for catalog operations such as
 * listing databases, tables, and managing metadata.
 */
public class FlussCatalogOptions {

    public static final Option<String> DEFAULT_DATABASE =
            Options.key("default-database")
                    .stringType()
                    .defaultValue("fluss")
                    .withDescription("Default database name used when none is specified.");

    private FlussCatalogOptions() {}
}
