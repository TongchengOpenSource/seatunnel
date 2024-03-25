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

package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Getter;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalogFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Getter
@Builder
public class StarRocksSourceTableConfig implements Serializable {

  private final TablePath tablePath;

  private final CatalogTable catalogTable;

  private String scanFilter;

  private StarRocksSourceTableConfig(String tablePath, CatalogTable catalogTable, String scanFilter) {
    this.tablePath = TablePath.of(tablePath);
    this.catalogTable = catalogTable;
    this.scanFilter = scanFilter;
  }

  public static StarRocksSourceTableConfig parseStarRocksSourceConfig(ReadonlyConfig config, StarRocksCatalog starRocksCatalog) {
    CatalogTable catalogTable;
    String tableName = config.get(CommonConfig.TABLE);
    if (config.getOptional(TableSchemaOptions.SCHEMA).isPresent()) {
      catalogTable = CatalogTableUtil.buildWithConfig(config);
    } else {
      catalogTable = starRocksCatalog.getTable(TablePath.of(config.get(CommonConfig.TABLE)));
    }
    String scanFilter = config.get(SourceConfig.SCAN_FILTER);
    return new StarRocksSourceTableConfig(tableName, catalogTable, scanFilter);
  }

  public static List<StarRocksSourceTableConfig> of(ReadonlyConfig config) {

    Optional<Catalog> optionalCatalog =
        FactoryUtil.createOptionalCatalog(
            StarRocksCatalogFactory.IDENTIFIER,
            config,
            StarRocksSourceTableConfig.class.getClassLoader(),
            StarRocksCatalogFactory.IDENTIFIER);

    try (StarRocksCatalog starRocksCatalog = (StarRocksCatalog) optionalCatalog.get()) {
      starRocksCatalog.open();
      if (config.getOptional(SourceConfig.TABLE_LIST).isPresent()) {
        return config.get(SourceConfig.TABLE_LIST).stream()
                .map(ReadonlyConfig::fromMap)
                .map(readonlyConfig -> parseStarRocksSourceConfig(readonlyConfig, starRocksCatalog))
                .collect(Collectors.toList());
      }
      StarRocksSourceTableConfig starRocksSourceTableConfig =
              parseStarRocksSourceConfig(config, starRocksCatalog);
      return Lists.newArrayList(starRocksSourceTableConfig);
    }
  }
}
