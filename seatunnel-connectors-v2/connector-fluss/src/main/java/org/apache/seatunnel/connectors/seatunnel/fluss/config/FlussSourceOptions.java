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

/**
 * Source-specific options for Fluss connector, following the official Fluss design pattern.
 *
 * <p>This class only defines source-specific options that are not covered by the core Fluss
 * configuration. Performance and client-related options should be configured through the
 * fluss.config map.
 */
public class FlussSourceOptions extends FlussOptions {

    // ----------------------------------------------------------------------------------------
    // Scan startup options
    // ----------------------------------------------------------------------------------------

    public static final Option<StartupMode> SCAN_STARTUP_MODE =
            Options.key("scan.startup.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.FULL)
                    .withDescription(
                            "Optional startup mode for Fluss source. Default is 'full'. "
                                    + "Options: "
                                    + "'full' - performs a full snapshot on the table upon first startup, "
                                    + "and continue to read the latest changelog with exactly once guarantee; "
                                    + "'earliest' - start reading logs from the earliest offset; "
                                    + "'latest' - start reading logs from the latest offset; "
                                    + "'timestamp' - start reading logs from user-supplied timestamp.");

    public static final Option<String> SCAN_STARTUP_TIMESTAMP =
            Options.key("scan.startup.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp for Fluss source in case of startup mode is timestamp. "
                                    + "The format is 'timestamp' or 'yyyy-MM-dd HH:mm:ss'. "
                                    + "Like '1678883047356' or '2023-12-09 23:09:12'.");
}
