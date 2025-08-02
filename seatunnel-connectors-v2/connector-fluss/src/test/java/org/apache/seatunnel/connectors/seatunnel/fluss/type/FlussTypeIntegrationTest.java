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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.types.DataTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public
/**
 * Integration test for Fluss type conversion system. Tests the complete flow from Fluss types to
 * SeaTunnel types and back.
 */
static class FlussTypeIntegrationTest {

    @Test
    public void testCompleteTypeConversionFlow() {
        // Create a comprehensive Fluss schema with all supported types
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Basic types
        schemaBuilder.column("col_boolean", DataTypes.BOOLEAN(), true, "Boolean column");
        schemaBuilder.column("col_tinyint", DataTypes.TINYINT(), true, "TinyInt column");
        schemaBuilder.column("col_smallint", DataTypes.SMALLINT(), true, "SmallInt column");
        schemaBuilder.column("col_int", DataTypes.INT(), false, "Int column");
        schemaBuilder.column("col_bigint", DataTypes.BIGINT(), true, "BigInt column");
        schemaBuilder.column("col_float", DataTypes.FLOAT(), true, "Float column");
        schemaBuilder.column("col_double", DataTypes.DOUBLE(), true, "Double column");

        // String types
        schemaBuilder.column("col_char", DataTypes.CHAR(50), true, "Char column");
        schemaBuilder.column("col_string", DataTypes.STRING(), true, "String column");

        // Decimal type
        schemaBuilder.column("col_decimal", DataTypes.DECIMAL(18, 4), true, "Decimal column");

        // Date/Time types
        schemaBuilder.column("col_date", DataTypes.DATE(), true, "Date column");
        schemaBuilder.column("col_time", DataTypes.TIME(6), true, "Time column");
        schemaBuilder.column("col_timestamp", DataTypes.TIMESTAMP(9), true, "Timestamp column");
        schemaBuilder.column(
                "col_timestamp_ltz", DataTypes.TIMESTAMP_LTZ(3), true, "Timestamp LTZ column");

        // Binary types
        schemaBuilder.column("col_binary", DataTypes.BINARY(100), true, "Binary column");
        schemaBuilder.column("col_bytes", DataTypes.BYTES(), true, "Bytes column");

        // Complex types
        schemaBuilder.column(
                "col_array", DataTypes.ARRAY(DataTypes.STRING()), true, "Array column");
        schemaBuilder.column(
                "col_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), true, "Map column");
        schemaBuilder.column(
                "col_row",
                DataTypes.ROW(
                        DataTypes.FIELD("nested_id", DataTypes.BIGINT()),
                        DataTypes.FIELD("nested_name", DataTypes.STRING()),
                        DataTypes.FIELD("nested_score", DataTypes.DECIMAL(10, 2))),
                true,
                "Row column");

        // Set primary key
        schemaBuilder.primaryKey("col_int");

        Schema schema = schemaBuilder.build();

        // Create TableDescriptor
        Map<String, String> properties = new HashMap<>();
        properties.put("bucket.num", "8");
        properties.put("log.retention.time", "1h");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .properties(properties)
                        .comment("Integration test table")
                        .build();

        // Convert to SeaTunnel CatalogTable
        CatalogTable catalogTable = FlussTypeConverter.toSeaTunnelTable(tableDescriptor);

        // Verify the conversion
        assertNotNull(catalogTable);
        assertEquals("Integration test table", catalogTable.getComment());

        TableSchema tableSchema = catalogTable.getTableSchema();
        assertNotNull(tableSchema);

        List<Column> columns = tableSchema.getColumns();
        assertEquals(18, columns.size()); // Total number of columns

        // Verify basic types
        verifyColumn(columns.get(0), "col_boolean", BasicType.BOOLEAN_TYPE, true, "Boolean column");
        verifyColumn(columns.get(1), "col_tinyint", BasicType.BYTE_TYPE, true, "TinyInt column");
        verifyColumn(columns.get(2), "col_smallint", BasicType.SHORT_TYPE, true, "SmallInt column");
        verifyColumn(columns.get(3), "col_int", BasicType.INT_TYPE, false, "Int column");
        verifyColumn(columns.get(4), "col_bigint", BasicType.LONG_TYPE, true, "BigInt column");
        verifyColumn(columns.get(5), "col_float", BasicType.FLOAT_TYPE, true, "Float column");
        verifyColumn(columns.get(6), "col_double", BasicType.DOUBLE_TYPE, true, "Double column");

        // Verify string types
        verifyColumn(columns.get(7), "col_char", BasicType.STRING_TYPE, true, "Char column");
        assertEquals(50L, columns.get(7).getColumnLength()); // Char length
        verifyColumn(columns.get(8), "col_string", BasicType.STRING_TYPE, true, "String column");

        // Verify decimal type
        Column decimalColumn = columns.get(9);
        assertEquals("col_decimal", decimalColumn.getName());
        assertTrue(decimalColumn.getDataType() instanceof DecimalType);
        DecimalType decimalType = (DecimalType) decimalColumn.getDataType();
        assertEquals(18, decimalType.getPrecision());
        assertEquals(4, decimalType.getScale());
        assertEquals("Decimal column", decimalColumn.getComment());

        // Verify date/time types
        verifyColumn(
                columns.get(10), "col_date", LocalTimeType.LOCAL_DATE_TYPE, true, "Date column");
        verifyColumn(
                columns.get(11), "col_time", LocalTimeType.LOCAL_TIME_TYPE, true, "Time column");
        assertEquals(6, columns.get(11).getScale()); // Time precision
        verifyColumn(
                columns.get(12),
                "col_timestamp",
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                true,
                "Timestamp column");
        assertEquals(9, columns.get(12).getScale()); // Timestamp precision
        verifyColumn(
                columns.get(13),
                "col_timestamp_ltz",
                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                true,
                "Timestamp LTZ column");
        assertEquals(3, columns.get(13).getScale()); // Timestamp LTZ precision

        // Verify binary types
        verifyColumn(
                columns.get(14),
                "col_binary",
                PrimitiveByteArrayType.INSTANCE,
                true,
                "Binary column");
        assertEquals(100L, columns.get(14).getColumnLength()); // Binary length
        verifyColumn(
                columns.get(15),
                "col_bytes",
                PrimitiveByteArrayType.INSTANCE,
                true,
                "Bytes column");

        // Verify complex types
        Column arrayColumn = columns.get(16);
        assertEquals("col_array", arrayColumn.getName());
        assertTrue(arrayColumn.getDataType() instanceof ArrayType);
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) arrayColumn.getDataType();
        assertEquals(BasicType.STRING_TYPE, arrayType.getElementType());

        Column mapColumn = columns.get(17);
        assertEquals("col_map", mapColumn.getName());
        assertTrue(mapColumn.getDataType() instanceof MapType);
        MapType<?, ?> mapType = (MapType<?, ?>) mapColumn.getDataType();
        assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        assertEquals(BasicType.INT_TYPE, mapType.getValueType());

        // Note: ROW type verification would be at index 18, but we only have 18 columns (0-17)
        // Let's check if we have the row column
        boolean foundRowColumn = false;
        for (Column column : columns) {
            if ("col_row".equals(column.getName())) {
                assertTrue(column.getDataType() instanceof SeaTunnelRowType);
                SeaTunnelRowType rowType = (SeaTunnelRowType) column.getDataType();
                assertEquals(3, rowType.getTotalFields());
                assertEquals("nested_id", rowType.getFieldName(0));
                assertEquals("nested_name", rowType.getFieldName(1));
                assertEquals("nested_score", rowType.getFieldName(2));
                assertEquals(BasicType.LONG_TYPE, rowType.getFieldType(0));
                assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(1));
                assertTrue(rowType.getFieldType(2) instanceof DecimalType);
                foundRowColumn = true;
                break;
            }
        }
        assertTrue(foundRowColumn, "Row column should be found");

        // Verify primary key
        assertNotNull(tableSchema.getPrimaryKey());
        assertEquals(1, tableSchema.getPrimaryKey().getColumnNames().size());
        assertEquals("col_int", tableSchema.getPrimaryKey().getColumnNames().get(0));

        // Verify options
        Map<String, String> options = catalogTable.getOptions();
        assertEquals("fluss", options.get("connector"));
        assertEquals("8", options.get("bucket.num"));
        assertEquals("1h", options.get("log.retention.time"));
    }

    @Test
    public void testTypeMapperIntegration() {
        FlussTypeConvert typeMapper = FlussTypeConvert.INSTANCE;

        // Test that the type mapper is properly registered and working
        assertEquals("fluss", typeMapper.identifier());

        // Test a complex conversion through the type mapper
        com.alibaba.fluss.types.DataType complexFlussType =
                DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.DECIMAL(15, 5)));

        org.apache.seatunnel.api.table.converter.BasicTypeDefine<com.alibaba.fluss.types.DataType>
                typeDefine =
                        org.apache.seatunnel.api.table.converter.BasicTypeDefine
                                .<com.alibaba.fluss.types.DataType>builder()
                                .name("complex_field")
                                .nativeType(complexFlussType)
                                .dataType("MAP")
                                .columnType("MAP<STRING,ARRAY<DECIMAL(15,5)>>")
                                .nullable(true)
                                .comment("Complex type test")
                                .build();

        Column column = typeMapper.convert(typeDefine);
        assertEquals("complex_field", column.getName());
        assertTrue(column.getDataType() instanceof MapType);

        MapType<?, ?> mapType = (MapType<?, ?>) column.getDataType();
        assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        assertTrue(mapType.getValueType() instanceof ArrayType);

        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) mapType.getValueType();
        assertTrue(arrayType.getElementType() instanceof DecimalType);

        DecimalType decimalType = (DecimalType) arrayType.getElementType();
        assertEquals(15, decimalType.getPrecision());
        assertEquals(5, decimalType.getScale());

        // Test reconvert
        org.apache.seatunnel.api.table.converter.BasicTypeDefine<com.alibaba.fluss.types.DataType>
                reconvertedDefine = typeMapper.reconvert(column);
        assertEquals("complex_field", reconvertedDefine.getName());
        assertTrue(reconvertedDefine.getNativeType() instanceof com.alibaba.fluss.types.MapType);
        assertEquals("Complex type test", reconvertedDefine.getComment());
    }

    @Test
    public void testRowTypeConverterIntegration() {
        // Test direct usage of FlussRowTypeConverter
        com.alibaba.fluss.types.DataType flussType = DataTypes.TIMESTAMP(6);

        org.apache.seatunnel.api.table.converter.BasicTypeDefine<com.alibaba.fluss.types.DataType>
                typeDefine =
                        org.apache.seatunnel.api.table.converter.BasicTypeDefine
                                .<com.alibaba.fluss.types.DataType>builder()
                                .name("test_timestamp")
                                .nativeType(flussType)
                                .dataType("TIMESTAMP")
                                .columnType("TIMESTAMP(6)")
                                .scale(6)
                                .nullable(false)
                                .comment("Test timestamp")
                                .build();

        Column column = FlussTypeConvert.INSTANCE.convert(typeDefine);
        assertEquals("test_timestamp", column.getName());
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        assertEquals(6, column.getScale());
        assertFalse(column.isNullable());
        assertEquals("Test timestamp", column.getComment());

        // Test reconvert
        org.apache.seatunnel.api.table.converter.BasicTypeDefine<com.alibaba.fluss.types.DataType>
                reconvertedDefine = FlussRowTypeConverter.reconvert(column);
        assertEquals("test_timestamp", reconvertedDefine.getName());
        assertTrue(
                reconvertedDefine.getNativeType() instanceof com.alibaba.fluss.types.TimestampType);
        assertEquals("TIMESTAMP", reconvertedDefine.getDataType());
        assertEquals("TIMESTAMP(6)", reconvertedDefine.getColumnType());
        assertEquals(6, reconvertedDefine.getScale());
        assertFalse(reconvertedDefine.isNullable());
        assertEquals("Test timestamp", reconvertedDefine.getComment());
    }

    private void verifyColumn(
            Column column,
            String expectedName,
            org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> expectedType,
            boolean expectedNullable,
            String expectedComment) {
        assertEquals(expectedName, column.getName());
        assertEquals(expectedType, column.getDataType());
        assertEquals(expectedNullable, column.isNullable());
        assertEquals(expectedComment, column.getComment());
    }
}
