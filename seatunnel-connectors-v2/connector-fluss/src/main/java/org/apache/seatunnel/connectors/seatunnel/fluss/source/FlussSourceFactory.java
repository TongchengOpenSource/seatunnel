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

package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceOptions.*;

@AutoService(Factory.class)
public class FlussSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Fluss";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BOOTSTRAP_SERVERS, DATABASE, TABLE)
                .optional(
                        SCAN_STARTUP_MODE,
                        SCAN_STARTUP_TIMESTAMP,
                        SCAN_PARALLELISM,
                        FETCH_SIZE,
                        POLL_TIMEOUT_MS,
                        ENABLE_CHANGELOG,
                        CONSUMER_GROUP,
                        CONNECTION_TIMEOUT_MS,
                        REQUEST_TIMEOUT_MS,
                        RETRY_TIMES,
                        RETRY_INTERVAL_MS,
                        FLUSS_CONFIG
                )
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return FlussSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig options = context.getOptions();
            CatalogTable catalogTable;

            if (options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                catalogTable = CatalogTableUtil.buildWithConfig(factoryIdentifier(), options);
            } else {
                catalogTable = CatalogTableUtil.buildSimpleTextTable();
            }

            return (SeaTunnelSource<T, SplitT, StateT>) new FlussSource(options, catalogTable);
        };
    }
}
