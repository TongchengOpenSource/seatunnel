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

package org.apache.seatunnel.connectors.seatunnel.fluss.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting between Fluss and SeaTunnel data types and table metadata.
 * 
 * <p>This class provides methods to convert:
 * <ul>
 *   <li>Fluss data types to SeaTunnel data types</li>
 *   <li>SeaTunnel data types to Fluss data types</li>
 *   <li>Fluss table metadata to SeaTunnel CatalogTable</li>
 *   <li>SeaTunnel CatalogTable to Fluss table metadata</li>
 * </ul>
 */
@Slf4j
public class FlussTypeConverter {

    private FlussTypeConverter() {
        // Utility class
    }

    /**
     * Convert Fluss DataType to SeaTunnel data type.
     *
     * @param flussDataType the Fluss DataType
     * @return the corresponding SeaTunnel data type
     */
    public static SeaTunnelDataType<?> toSeaTunnelType(DataType flussDataType) {
        if (flussDataType == null) {
            throw new IllegalArgumentException("Fluss DataType cannot be null");
        }

        if (flussDataType.equals(DataTypes.BOOLEAN())) {
            return BasicType.BOOLEAN_TYPE;
        } else if (flussDataType.equals(DataTypes.TINYINT())) {
            return BasicType.BYTE_TYPE;
        } else if (flussDataType.equals(DataTypes.SMALLINT())) {
            return BasicType.SHORT_TYPE;
        } else if (flussDataType.equals(DataTypes.INT())) {
            return BasicType.INT_TYPE;
        } else if (flussDataType.equals(DataTypes.BIGINT())) {
            return BasicType.LONG_TYPE;
        } else if (flussDataType.equals(DataTypes.FLOAT())) {
            return BasicType.FLOAT_TYPE;
        } else if (flussDataType.equals(DataTypes.DOUBLE())) {
            return BasicType.DOUBLE_TYPE;
        } else if (flussDataType.equals(DataTypes.STRING())) {
            return BasicType.STRING_TYPE;
        } else if (flussDataType.equals(DataTypes.BYTES())) {
            return PrimitiveByteArrayType.INSTANCE;
        } else if (flussDataType.equals(DataTypes.DATE())) {
            return LocalTimeType.LOCAL_DATE_TYPE;
        } else if (flussDataType.equals(DataTypes.TIME())) {
            return LocalTimeType.LOCAL_TIME_TYPE;
        } else if (flussDataType.equals(DataTypes.TIMESTAMP())) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        } else if (flussDataType instanceof com.alibaba.fluss.types.DecimalType) {
            com.alibaba.fluss.types.DecimalType flussDecimal =
                    (com.alibaba.fluss.types.DecimalType) flussDataType;
            return new DecimalType(flussDecimal.getPrecision(), flussDecimal.getScale());
        } else {
            log.warn("Unsupported Fluss type '{}', using STRING as fallback", flussDataType);
            return BasicType.STRING_TYPE;
        }
    }

    /**
     * Convert Fluss data type string to SeaTunnel data type (legacy method).
     *
     * @param flussType the Fluss data type string
     * @return the corresponding SeaTunnel data type
     */
    public static SeaTunnelDataType<?> toSeaTunnelType(String flussType) {
        if (flussType == null) {
            throw new IllegalArgumentException("Fluss type cannot be null");
        }

        String normalizedType = flussType.toLowerCase().trim();

        // Handle basic types
        switch (normalizedType) {
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "tinyint":
                return BasicType.BYTE_TYPE;
            case "smallint":
                return BasicType.SHORT_TYPE;
            case "int":
            case "integer":
                return BasicType.INT_TYPE;
            case "bigint":
                return BasicType.LONG_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "string":
            case "varchar":
                return BasicType.STRING_TYPE;
            case "binary":
            case "varbinary":
                return PrimitiveByteArrayType.INSTANCE;
            case "date":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "time":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "timestamp":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                // Handle complex types
                if (normalizedType.startsWith("decimal")) {
                    return parseDecimalType(normalizedType);
                } else if (normalizedType.startsWith("varchar")) {
                    return BasicType.STRING_TYPE; // Simplified for now
                } else if (normalizedType.startsWith("char")) {
                    return BasicType.STRING_TYPE; // Simplified for now
                } else {
                    log.warn("Unsupported Fluss type '{}', using STRING as fallback", flussType);
                    return BasicType.STRING_TYPE;
                }
        }
    }

    /**
     * Convert SeaTunnel data type to Fluss data type string.
     * 
     * @param seaTunnelType the SeaTunnel data type
     * @return the corresponding Fluss data type string
     */
    public static String toFlussType(SeaTunnelDataType<?> seaTunnelType) {
        if (seaTunnelType == null) {
            throw new IllegalArgumentException("SeaTunnel type cannot be null");
        }

        switch (seaTunnelType.getSqlType()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case STRING:
                return "STRING";
            case BYTES:
                return "BINARY";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) seaTunnelType;
                return String.format("DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            default:
                log.warn("Unsupported SeaTunnel type '{}', using STRING as fallback", seaTunnelType);
                return "STRING";
        }
    }

    /**
     * Convert Fluss TableDescriptor to SeaTunnel CatalogTable.
     *
     * @param tableDescriptor the Fluss TableDescriptor
     * @return the corresponding SeaTunnel CatalogTable
     */
    public static CatalogTable toSeaTunnelTable(TableDescriptor tableDescriptor) {
        if (tableDescriptor == null) {
            throw new IllegalArgumentException("TableDescriptor cannot be null");
        }

        Schema flussSchema = tableDescriptor.getSchema();

        // Convert columns
        List<Column> columns = new ArrayList<>();
        for (Schema.Column flussColumn : flussSchema.getColumns()) {
            SeaTunnelDataType<?> seaTunnelType = toSeaTunnelType(flussColumn.getDataType());
            Column column = Column.of(
                    flussColumn.getName(),
                    seaTunnelType,
                    flussColumn.isNullable(),
                    null, // default value - not available in Fluss schema
                    flussColumn.getComment().orElse(null)
            );
            columns.add(column);
        }

        // Build table schema
        TableSchema.Builder schemaBuilder = TableSchema.builder().columns(columns);

        // Set primary key if available
        if (flussSchema.getPrimaryKey().isPresent()) {
            List<String> primaryKeyColumns = flussSchema.getPrimaryKey().get().getColumnNames();
            if (!primaryKeyColumns.isEmpty()) {
                schemaBuilder.primaryKey(primaryKeyColumns.toArray(new String[0]));
            }
        }

        TableSchema schema = schemaBuilder.build();

        // Build options from table properties
        Map<String, String> options = new HashMap<>();
        options.put("connector", "fluss");

        // Add table properties as options
        tableDescriptor.getProperties().forEach((key, value) ->
                options.put(key, value.toString()));

        // Create table identifier
        // Note: We need to extract database and table name from somewhere
        // For now, use placeholder values
        TableIdentifier tableId = TableIdentifier.of("fluss", "default", "fluss_table");

        return CatalogTable.of(
                tableId,
                schema,
                options,
                new ArrayList<>(), // partition keys - TODO: extract from Fluss if available
                tableDescriptor.getComment().orElse("Fluss table")
        );
    }

    /**
     * Convert SeaTunnel CatalogTable to Fluss table metadata.
     * 
     * @param catalogTable the SeaTunnel CatalogTable
     * @return the corresponding Fluss table metadata (placeholder)
     */
    public static Object toFlussTable(CatalogTable catalogTable) {
        // TODO: Implement actual conversion from CatalogTable to Fluss TableDescriptor
        // This is a placeholder implementation
        
        log.info("Converting SeaTunnel CatalogTable to Fluss table: {}", catalogTable.getTableId());
        
        // For now, return a placeholder object
        return new Object();
    }

    // Helper methods

    private static SeaTunnelDataType<?> parseDecimalType(String decimalTypeStr) {
        try {
            // Parse DECIMAL(precision, scale) format
            if (decimalTypeStr.contains("(") && decimalTypeStr.contains(")")) {
                String params = decimalTypeStr.substring(
                        decimalTypeStr.indexOf("(") + 1, 
                        decimalTypeStr.indexOf(")"));
                String[] parts = params.split(",");
                
                if (parts.length == 2) {
                    int precision = Integer.parseInt(parts[0].trim());
                    int scale = Integer.parseInt(parts[1].trim());
                    return new DecimalType(precision, scale);
                } else if (parts.length == 1) {
                    int precision = Integer.parseInt(parts[0].trim());
                    return new DecimalType(precision, 0);
                }
            }
            
            // Default decimal type
            return new DecimalType(10, 0);
            
        } catch (Exception e) {
            log.warn("Failed to parse decimal type '{}', using default DECIMAL(10,0)", decimalTypeStr, e);
            return new DecimalType(10, 0);
        }
    }
}
