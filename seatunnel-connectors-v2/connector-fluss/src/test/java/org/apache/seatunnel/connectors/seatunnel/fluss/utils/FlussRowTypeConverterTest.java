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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;

public
/** Test cases for FlussRowTypeConverter to ensure all type conversions work correctly. */
static class FlussRowTypeConverterTest {

    @Test
    public void testBasicTypeConversion() {
        // Test BOOLEAN conversion
        BasicTypeDefine<DataType> booleanDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_boolean")
                        .nativeType(DataTypes.BOOLEAN())
                        .dataType("BOOLEAN")
                        .columnType("BOOLEAN")
                        .nullable(true)
                        .comment("Test boolean field")
                        .build();

        Column booleanColumn = FlussRowTypeConverter.convert(booleanDefine);
        assertEquals("test_boolean", booleanColumn.getName());
        assertEquals(BasicType.BOOLEAN_TYPE, booleanColumn.getDataType());
        assertTrue(booleanColumn.isNullable());
        assertEquals("Test boolean field", booleanColumn.getComment());
    }

    @Test
    public void testNumericTypeConversions() {
        // Test TINYINT
        BasicTypeDefine<DataType> tinyintDefine =
                createBasicTypeDefine("test_tinyint", DataTypes.TINYINT(), "TINYINT");
        Column tinyintColumn = FlussRowTypeConverter.convert(tinyintDefine);
        assertEquals(BasicType.BYTE_TYPE, tinyintColumn.getDataType());

        // Test SMALLINT
        BasicTypeDefine<DataType> smallintDefine =
                createBasicTypeDefine("test_smallint", DataTypes.SMALLINT(), "SMALLINT");
        Column smallintColumn = FlussRowTypeConverter.convert(smallintDefine);
        assertEquals(BasicType.SHORT_TYPE, smallintColumn.getDataType());

        // Test INT
        BasicTypeDefine<DataType> intDefine =
                createBasicTypeDefine("test_int", DataTypes.INT(), "INT");
        Column intColumn = FlussRowTypeConverter.convert(intDefine);
        assertEquals(BasicType.INT_TYPE, intColumn.getDataType());

        // Test BIGINT
        BasicTypeDefine<DataType> bigintDefine =
                createBasicTypeDefine("test_bigint", DataTypes.BIGINT(), "BIGINT");
        Column bigintColumn = FlussRowTypeConverter.convert(bigintDefine);
        assertEquals(BasicType.LONG_TYPE, bigintColumn.getDataType());

        // Test FLOAT
        BasicTypeDefine<DataType> floatDefine =
                createBasicTypeDefine("test_float", DataTypes.FLOAT(), "FLOAT");
        Column floatColumn = FlussRowTypeConverter.convert(floatDefine);
        assertEquals(BasicType.FLOAT_TYPE, floatColumn.getDataType());

        // Test DOUBLE
        BasicTypeDefine<DataType> doubleDefine =
                createBasicTypeDefine("test_double", DataTypes.DOUBLE(), "DOUBLE");
        Column doubleColumn = FlussRowTypeConverter.convert(doubleDefine);
        assertEquals(BasicType.DOUBLE_TYPE, doubleColumn.getDataType());
    }

    @Test
    public void testStringTypeConversions() {
        // Test CHAR with length
        BasicTypeDefine<DataType> charDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_char")
                        .nativeType(DataTypes.CHAR(10))
                        .dataType("CHAR")
                        .columnType("CHAR(10)")
                        .length(10L)
                        .nullable(true)
                        .build();

        Column charColumn = FlussRowTypeConverter.convert(charDefine);
        assertEquals("test_char", charColumn.getName());
        assertEquals(BasicType.STRING_TYPE, charColumn.getDataType());
        assertEquals(10L, charColumn.getColumnLength());

        // Test STRING
        BasicTypeDefine<DataType> stringDefine =
                createBasicTypeDefine("test_string", DataTypes.STRING(), "STRING");
        Column stringColumn = FlussRowTypeConverter.convert(stringDefine);
        assertEquals(BasicType.STRING_TYPE, stringColumn.getDataType());
    }

    @Test
    public void testDecimalTypeConversion() {
        // Test DECIMAL with precision and scale
        BasicTypeDefine<DataType> decimalDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_decimal")
                        .nativeType(DataTypes.DECIMAL(10, 2))
                        .dataType("DECIMAL")
                        .columnType("DECIMAL(10,2)")
                        .precision(10L)
                        .scale(2)
                        .nullable(true)
                        .build();

        Column decimalColumn = FlussRowTypeConverter.convert(decimalDefine);
        assertEquals("test_decimal", decimalColumn.getName());
        assertTrue(decimalColumn.getDataType() instanceof DecimalType);
        DecimalType decimalType = (DecimalType) decimalColumn.getDataType();
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
        assertEquals(10L, decimalColumn.getColumnLength());
        assertEquals(2, decimalColumn.getScale());
    }

    @Test
    public void testDateTimeTypeConversions() {
        // Test DATE
        BasicTypeDefine<DataType> dateDefine =
                createBasicTypeDefine("test_date", DataTypes.DATE(), "DATE");
        Column dateColumn = FlussRowTypeConverter.convert(dateDefine);
        assertEquals(LocalTimeType.LOCAL_DATE_TYPE, dateColumn.getDataType());

        // Test TIME with precision
        BasicTypeDefine<DataType> timeDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_time")
                        .nativeType(DataTypes.TIME(6))
                        .dataType("TIME")
                        .columnType("TIME(6)")
                        .scale(6)
                        .nullable(true)
                        .build();

        Column timeColumn = FlussRowTypeConverter.convert(timeDefine);
        assertEquals(LocalTimeType.LOCAL_TIME_TYPE, timeColumn.getDataType());
        assertEquals(6, timeColumn.getScale());

        // Test TIMESTAMP with precision
        BasicTypeDefine<DataType> timestampDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_timestamp")
                        .nativeType(DataTypes.TIMESTAMP(9))
                        .dataType("TIMESTAMP")
                        .columnType("TIMESTAMP(9)")
                        .scale(9)
                        .nullable(true)
                        .build();

        Column timestampColumn = FlussRowTypeConverter.convert(timestampDefine);
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, timestampColumn.getDataType());
        assertEquals(9, timestampColumn.getScale());

        // Test TIMESTAMP_LTZ
        BasicTypeDefine<DataType> timestampLtzDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_timestamp_ltz")
                        .nativeType(DataTypes.TIMESTAMP_LTZ(3))
                        .dataType("TIMESTAMP_LTZ")
                        .columnType("TIMESTAMP_LTZ(3)")
                        .scale(3)
                        .nullable(true)
                        .build();

        Column timestampLtzColumn = FlussRowTypeConverter.convert(timestampLtzDefine);
        assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, timestampLtzColumn.getDataType());
        assertEquals(3, timestampLtzColumn.getScale());
    }

    @Test
    public void testBinaryTypeConversions() {
        // Test BINARY with length
        BasicTypeDefine<DataType> binaryDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_binary")
                        .nativeType(DataTypes.BINARY(100))
                        .dataType("BINARY")
                        .columnType("BINARY(100)")
                        .length(100L)
                        .nullable(true)
                        .build();

        Column binaryColumn = FlussRowTypeConverter.convert(binaryDefine);
        assertEquals("test_binary", binaryColumn.getName());
        assertEquals(PrimitiveByteArrayType.INSTANCE, binaryColumn.getDataType());
        assertEquals(100L, binaryColumn.getColumnLength());

        // Test BYTES
        BasicTypeDefine<DataType> bytesDefine =
                createBasicTypeDefine("test_bytes", DataTypes.BYTES(), "BYTES");
        Column bytesColumn = FlussRowTypeConverter.convert(bytesDefine);
        assertEquals(PrimitiveByteArrayType.INSTANCE, bytesColumn.getDataType());
    }

    @Test
    public void testComplexTypeConversions() {
        // Test ARRAY
        BasicTypeDefine<DataType> arrayDefine =
                createBasicTypeDefine(
                        "test_array", DataTypes.ARRAY(DataTypes.STRING()), "ARRAY<STRING>");
        Column arrayColumn = FlussRowTypeConverter.convert(arrayDefine);
        assertTrue(arrayColumn.getDataType() instanceof ArrayType);
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) arrayColumn.getDataType();
        assertEquals(BasicType.STRING_TYPE, arrayType.getElementType());

        // Test MAP
        BasicTypeDefine<DataType> mapDefine =
                createBasicTypeDefine(
                        "test_map",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        "MAP<STRING,INT>");
        Column mapColumn = FlussRowTypeConverter.convert(mapDefine);
        assertTrue(mapColumn.getDataType() instanceof MapType);
        MapType<?, ?> mapType = (MapType<?, ?>) mapColumn.getDataType();
        assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        assertEquals(BasicType.INT_TYPE, mapType.getValueType());

        // Test ROW
        BasicTypeDefine<DataType> rowDefine =
                createBasicTypeDefine(
                        "test_row",
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("name", DataTypes.STRING())),
                        "ROW<id BIGINT, name STRING>");
        Column rowColumn = FlussRowTypeConverter.convert(rowDefine);
        assertTrue(rowColumn.getDataType() instanceof SeaTunnelRowType);
        SeaTunnelRowType rowType = (SeaTunnelRowType) rowColumn.getDataType();
        assertEquals(2, rowType.getTotalFields());
        assertEquals("id", rowType.getFieldName(0));
        assertEquals("name", rowType.getFieldName(1));
        assertEquals(BasicType.LONG_TYPE, rowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(1));
    }

    @Test
    public void testReconvertFromSeaTunnelColumn() {
        // Test reconvert from SeaTunnel Column to Fluss BasicTypeDefine

        // Test BOOLEAN
        Column booleanColumn =
                PhysicalColumn.of(
                        "test_boolean", BasicType.BOOLEAN_TYPE, 0L, true, null, "Test boolean");
        BasicTypeDefine<DataType> booleanDefine = FlussRowTypeConverter.reconvert(booleanColumn);
        assertEquals("test_boolean", booleanDefine.getName());
        assertTrue(booleanDefine.getNativeType() instanceof com.alibaba.fluss.types.BooleanType);
        assertEquals("BOOLEAN", booleanDefine.getDataType());
        assertEquals("BOOLEAN", booleanDefine.getColumnType());

        // Test DECIMAL
        Column decimalColumn =
                PhysicalColumn.of(
                        "test_decimal", new DecimalType(10, 2), 10L, true, null, "Test decimal");
        decimalColumn =
                PhysicalColumn.builder()
                        .name("test_decimal")
                        .dataType(new DecimalType(10, 2))
                        .columnLength(10L)
                        .scale(2)
                        .nullable(true)
                        .comment("Test decimal")
                        .build();
        BasicTypeDefine<DataType> decimalDefine = FlussRowTypeConverter.reconvert(decimalColumn);
        assertEquals("test_decimal", decimalDefine.getName());
        assertTrue(decimalDefine.getNativeType() instanceof com.alibaba.fluss.types.DecimalType);
        assertEquals("DECIMAL", decimalDefine.getDataType());
        assertEquals("DECIMAL(10,2)", decimalDefine.getColumnType());
        assertEquals(10L, decimalDefine.getPrecision());
        assertEquals(2, decimalDefine.getScale());

        // Test STRING with length (should convert to CHAR)
        Column charColumn =
                PhysicalColumn.builder()
                        .name("test_char")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(50L)
                        .nullable(true)
                        .comment("Test char")
                        .build();
        BasicTypeDefine<DataType> charDefine = FlussRowTypeConverter.reconvert(charColumn);
        assertEquals("test_char", charDefine.getName());
        assertTrue(charDefine.getNativeType() instanceof com.alibaba.fluss.types.CharType);
        assertEquals("CHAR", charDefine.getDataType());
        assertEquals("CHAR(50)", charDefine.getColumnType());
        assertEquals(50L, charDefine.getLength());

        // Test STRING without length (should convert to STRING)
        Column stringColumn =
                PhysicalColumn.of(
                        "test_string", BasicType.STRING_TYPE, 0L, true, null, "Test string");
        BasicTypeDefine<DataType> stringDefine = FlussRowTypeConverter.reconvert(stringColumn);
        assertEquals("test_string", stringDefine.getName());
        assertTrue(stringDefine.getNativeType() instanceof com.alibaba.fluss.types.StringType);
        assertEquals("STRING", stringDefine.getDataType());
        assertEquals("STRING", stringDefine.getColumnType());
    }

    @Test
    public void testReconvertDataType() {
        // Test reconvert from SeaTunnel DataType to Fluss DataType

        // Test basic types
        DataType booleanType =
                FlussRowTypeConverter.reconvert("test_field", BasicType.BOOLEAN_TYPE);
        assertTrue(booleanType instanceof com.alibaba.fluss.types.BooleanType);

        DataType intType = FlussRowTypeConverter.reconvert("test_field", BasicType.INT_TYPE);
        assertTrue(intType instanceof com.alibaba.fluss.types.IntType);

        DataType stringType = FlussRowTypeConverter.reconvert("test_field", BasicType.STRING_TYPE);
        assertTrue(stringType instanceof com.alibaba.fluss.types.StringType);

        // Test decimal
        DataType decimalType =
                FlussRowTypeConverter.reconvert("test_field", new DecimalType(10, 2));
        assertTrue(decimalType instanceof com.alibaba.fluss.types.DecimalType);
        com.alibaba.fluss.types.DecimalType flussDecimal =
                (com.alibaba.fluss.types.DecimalType) decimalType;
        assertEquals(10, flussDecimal.getPrecision());
        assertEquals(2, flussDecimal.getScale());

        // Test array
        ArrayType<String[], String> arrayType = ArrayType.STRING_ARRAY_TYPE;
        DataType flussArrayType = FlussRowTypeConverter.reconvert("test_field", arrayType);
        assertTrue(flussArrayType instanceof com.alibaba.fluss.types.ArrayType);
        com.alibaba.fluss.types.ArrayType flussArray =
                (com.alibaba.fluss.types.ArrayType) flussArrayType;
        assertTrue(flussArray.getElementType() instanceof com.alibaba.fluss.types.StringType);

        // Test map
        MapType<String, Integer> mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE);
        DataType flussMapType = FlussRowTypeConverter.reconvert("test_field", mapType);
        assertTrue(flussMapType instanceof com.alibaba.fluss.types.MapType);
        com.alibaba.fluss.types.MapType flussMap = (com.alibaba.fluss.types.MapType) flussMapType;
        assertTrue(flussMap.getKeyType() instanceof com.alibaba.fluss.types.StringType);
        assertTrue(flussMap.getValueType() instanceof com.alibaba.fluss.types.IntType);

        // Test row
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.LONG_TYPE, BasicType.STRING_TYPE
                        });
        DataType flussRowType = FlussRowTypeConverter.reconvert("test_field", rowType);
        assertTrue(flussRowType instanceof com.alibaba.fluss.types.RowType);
        com.alibaba.fluss.types.RowType flussRow = (com.alibaba.fluss.types.RowType) flussRowType;
        assertEquals(2, flussRow.getFieldCount());
        assertEquals("id", flussRow.getFieldNames().get(0));
        assertEquals("name", flussRow.getFieldNames().get(1));
        assertTrue(flussRow.getTypeAt(0) instanceof com.alibaba.fluss.types.BigIntType);
        assertTrue(flussRow.getTypeAt(1) instanceof com.alibaba.fluss.types.StringType);
    }

    private BasicTypeDefine<DataType> createBasicTypeDefine(
            String name, DataType nativeType, String dataType) {
        return BasicTypeDefine.<DataType>builder()
                .name(name)
                .nativeType(nativeType)
                .dataType(dataType)
                .columnType(dataType)
                .nullable(true)
                .build();
    }
}
