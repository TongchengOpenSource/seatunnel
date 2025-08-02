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

package org.apache.seatunnel.connectors.seatunnel.fluss.type;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussOptions;

import com.alibaba.fluss.types.DataType;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.seatunnel.fluss.type.visitor.FlussToSeaTunnelTypeVisitor;
import org.apache.seatunnel.connectors.seatunnel.fluss.type.visitor.SeaTunnelToFlussTypeVisitor;

@Slf4j
@AutoService(TypeConverter.class)
public class FlussTypeConvert implements TypeConverter<BasicTypeDefine<DataType>> {

    public static final FlussTypeConvert INSTANCE = new FlussTypeConvert();

    private FlussTypeConvert() {}

    @Override
    public String identifier() {
        return FlussOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Column convert(BasicTypeDefine<DataType> typeDefine) {
        if (typeDefine == null) {
            throw new IllegalArgumentException("Type definition cannot be null");
        }
        try {
            Column column =
                    PhysicalColumn.builder()
                            .name(typeDefine.getName())
                            .sourceType(typeDefine.getColumnType())
                            .nullable(typeDefine.isNullable())
                            .defaultValue(typeDefine.getDefaultValue())
                            .dataType(
                                    typeDefine
                                            .getNativeType()
                                            .accept(FlussToSeaTunnelTypeVisitor.INSTANCE))
                            .comment(typeDefine.getComment())
                            .build();
            log.info(
                    "Successfully converted to Column: {} with type {}",
                    column.getName(),
                    column.getDataType());
            return column;
        } catch (Exception e) {
            log.error("Failed to convert Fluss type definition: {}", typeDefine, e);
            throw e;
        }
    }

    @Override
    public BasicTypeDefine<DataType> reconvert(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("Column cannot be null");
        }
        try {
            BasicTypeDefine<DataType> typeDefine =
                    SeaTunnelToFlussTypeVisitor.INSTANCE.visitColumn(column);
            log.info("Successfully reconverted to Fluss type: {}", typeDefine.getDataType());
            return typeDefine;
        } catch (Exception e) {
            log.error("Failed to reconvert SeaTunnel Column: {}", column, e);
            throw e;
        }
    }

    @Override
    public String toString() {
        return "FlussTypeConvert{identifier='" + identifier() + "'}";
    }
}
