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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * Configuration for Fluss sink connector, simplified following the official Fluss design pattern.
 *
 * <p>This configuration only holds essential sink-specific settings. Other configurations are
 * handled through the fluss.config map to maintain compatibility with Fluss core.
 */
@Data
public class FlussSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String bootstrapServers;
    private String database;
    private String table;
    private Map<String, Object> flussProperties;

    public FlussSinkConfig(ReadonlyConfig config) {
        // Validate configuration first
        FlussConnectorOptionsUtils.validateSinkOptions(config);

        // Extract core configuration
        this.bootstrapServers = config.get(FlussOptions.BOOTSTRAP_SERVERS);
        this.database = config.get(FlussOptions.DATABASE);
        this.table = config.get(FlussOptions.TABLE);

        // Build Fluss properties
        this.flussProperties = FlussConnectorOptionsUtils.buildFlussProperties(config);
    }

    /**
     * Get the full table name in the format "database.table".
     *
     * @return the full table name
     */
    public String getFullTableName() {
        return database + "." + table;
    }
}
