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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.seatunnel.connectors.seatunnel.fluss.type.visitor.FlussToSeaTunnelTypeVisitor;
import org.apache.seatunnel.connectors.seatunnel.fluss.type.visitor.SeaTunnelToFlussTypeVisitor;
import org.junit.jupiter.api.Test;

import com.alibaba.fluss.types.DataType;

public
/** Test cases for SeaTunnelToFlussTypeVisitor to ensure all type conversions work correctly. */
static class SeaTunnelToFlussTypeVisitorTest {

    private final SeaTunnelToFlussTypeVisitor visitor = SeaTunnelToFlussTypeVisitor.INSTANCE;

    @Test
    public void testBasicTypeConversions() {
        // Test BOOLEAN
        DataType booleanType = visitor.visit("test_field", BasicType.BOOLEAN_TYPE);
        assertTrue(booleanType instanceof com.alibaba.fluss.types.BooleanType);

        // Test TINYINT
        DataType tinyintType = visitor.visit("test_field", BasicType.BYTE_TYPE);
        assertTrue(tinyintType instanceof com.alibaba.fluss.types.TinyIntType);

        // Test SMALLINT
        DataType smallintType = visitor.visit("test_field", BasicType.SHORT_TYPE);
        assertTrue(smallintType instanceof com.alibaba.fluss.types.SmallIntType);

        // Test INT
        DataType intType = visitor.visit("test_field", BasicType.INT_TYPE);
        assertTrue(intType instanceof com.alibaba.fluss.types.IntType);

        // Test BIGINT
        DataType bigintType = visitor.visit("test_field", BasicType.LONG_TYPE);
        assertTrue(bigintType instanceof com.alibaba.fluss.types.BigIntType);

        // Test FLOAT
        DataType floatType = visitor.visit("test_field", BasicType.FLOAT_TYPE);
        assertTrue(floatType instanceof com.alibaba.fluss.types.FloatType);

        // Test DOUBLE
        DataType doubleType = visitor.visit("test_field", BasicType.DOUBLE_TYPE);
        assertTrue(doubleType instanceof com.alibaba.fluss.types.DoubleType);

        // Test STRING
        DataType stringType = visitor.visit("test_field", BasicType.STRING_TYPE);
        assertTrue(stringType instanceof com.alibaba.fluss.types.StringType);
    }

    @Test
    public void testDecimalTypeConversion() {
        DecimalType seaTunnelDecimal = new DecimalType(10, 2);
        DataType flussDecimal = visitor.visit("test_decimal", seaTunnelDecimal);

        assertTrue(flussDecimal instanceof com.alibaba.fluss.types.DecimalType);
        com.alibaba.fluss.types.DecimalType flussDecimalType =
                (com.alibaba.fluss.types.DecimalType) flussDecimal;
        assertEquals(10, flussDecimalType.getPrecision());
        assertEquals(2, flussDecimalType.getScale());
    }

    @Test
    public void testLocalTimeTypeConversions() {
        // Test DATE
        DataType dateType = visitor.visit("test_date", LocalTimeType.LOCAL_DATE_TYPE);
        assertTrue(dateType instanceof com.alibaba.fluss.types.DateType);

        // Test TIME
        DataType timeType = visitor.visit("test_time", LocalTimeType.LOCAL_TIME_TYPE);
        assertTrue(timeType instanceof com.alibaba.fluss.types.TimeType);

        // Test TIMESTAMP
        DataType timestampType =
                visitor.visit("test_timestamp", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        assertTrue(timestampType instanceof com.alibaba.fluss.types.TimestampType);

        // Test TIMESTAMP_TZ
        DataType timestampLtzType =
                visitor.visit("test_timestamp_ltz", LocalTimeType.OFFSET_DATE_TIME_TYPE);
        assertTrue(timestampLtzType instanceof com.alibaba.fluss.types.LocalZonedTimestampType);
    }

    @Test
    public void testBinaryTypeConversion() {
        DataType bytesType = visitor.visit("test_bytes", PrimitiveByteArrayType.INSTANCE);
        assertTrue(bytesType instanceof com.alibaba.fluss.types.BytesType);
    }

    @Test
    public void testComplexTypeConversions() {
        // Test ARRAY
        ArrayType<String[], String> arrayType = ArrayType.STRING_ARRAY_TYPE;
        DataType flussArrayType = visitor.visit("test_array", arrayType);
        assertTrue(flussArrayType instanceof com.alibaba.fluss.types.ArrayType);
        com.alibaba.fluss.types.ArrayType flussArray =
                (com.alibaba.fluss.types.ArrayType) flussArrayType;
        assertTrue(flussArray.getElementType() instanceof com.alibaba.fluss.types.StringType);

        // Test MAP
        MapType<String, Integer> mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE);
        DataType flussMapType = visitor.visit("test_map", mapType);
        assertTrue(flussMapType instanceof com.alibaba.fluss.types.MapType);
        com.alibaba.fluss.types.MapType flussMap = (com.alibaba.fluss.types.MapType) flussMapType;
        assertTrue(flussMap.getKeyType() instanceof com.alibaba.fluss.types.StringType);
        assertTrue(flussMap.getValueType() instanceof com.alibaba.fluss.types.IntType);

        // Test ROW
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.LONG_TYPE, BasicType.STRING_TYPE
                        });
        DataType flussRowType = visitor.visit("test_row", rowType);
        assertTrue(flussRowType instanceof com.alibaba.fluss.types.RowType);
        com.alibaba.fluss.types.RowType flussRow = (com.alibaba.fluss.types.RowType) flussRowType;
        assertEquals(2, flussRow.getFieldCount());
        assertEquals("id", flussRow.getFieldNames().get(0));
        assertEquals("name", flussRow.getFieldNames().get(1));
        assertTrue(flussRow.getTypeAt(0) instanceof com.alibaba.fluss.types.BigIntType);
        assertTrue(flussRow.getTypeAt(1) instanceof com.alibaba.fluss.types.StringType);
    }

    @Test
    public void testVisitColumnMethod() {
        // Test basic column conversion
        Column booleanColumn =
                PhysicalColumn.of(
                        "test_boolean", BasicType.BOOLEAN_TYPE, 0L, true, null, "Test boolean");
        BasicTypeDefine<DataType> booleanDefine = visitor.visitColumn(booleanColumn);
        assertEquals("test_boolean", booleanDefine.getName());
        assertTrue(booleanDefine.getNativeType() instanceof com.alibaba.fluss.types.BooleanType);
        assertEquals("BOOLEAN", booleanDefine.getDataType());
        assertEquals("BOOLEAN", booleanDefine.getColumnType());

        // Test decimal column with precision and scale
        Column decimalColumn =
                PhysicalColumn.builder()
                        .name("test_decimal")
                        .dataType(new DecimalType(15, 3))
                        .columnLength(15L)
                        .scale(3)
                        .nullable(true)
                        .comment("Test decimal")
                        .build();

        BasicTypeDefine<DataType> decimalDefine = visitor.visitColumn(decimalColumn);
        assertEquals("test_decimal", decimalDefine.getName());
        assertTrue(decimalDefine.getNativeType() instanceof com.alibaba.fluss.types.DecimalType);
        assertEquals("DECIMAL", decimalDefine.getDataType());
        assertEquals("DECIMAL(15,3)", decimalDefine.getColumnType());
        assertEquals(15L, decimalDefine.getPrecision());
        assertEquals(3, decimalDefine.getScale());

        // Test string column with length (should convert to CHAR)
        Column charColumn =
                PhysicalColumn.builder()
                        .name("test_char")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(100L)
                        .nullable(false)
                        .comment("Test char field")
                        .build();

        BasicTypeDefine<DataType> charDefine = visitor.visitColumn(charColumn);
        assertEquals("test_char", charDefine.getName());
        assertTrue(charDefine.getNativeType() instanceof com.alibaba.fluss.types.CharType);
        assertEquals("CHAR", charDefine.getDataType());
        assertEquals("CHAR(100)", charDefine.getColumnType());
        assertEquals(100L, charDefine.getLength());

        // Test string column without length (should convert to STRING)
        Column stringColumn =
                PhysicalColumn.of(
                        "test_string", BasicType.STRING_TYPE, 0L, true, null, "Test string");
        BasicTypeDefine<DataType> stringDefine = visitor.visitColumn(stringColumn);
        assertEquals("test_string", stringDefine.getName());
        assertTrue(stringDefine.getNativeType() instanceof com.alibaba.fluss.types.StringType);
        assertEquals("STRING", stringDefine.getDataType());
        assertEquals("STRING", stringDefine.getColumnType());
    }

    @Test
    public void testConvenienceMethod() {
        // Test visit without field name
        DataType result = visitor.visit(BasicType.INT_TYPE);
        assertTrue(result instanceof com.alibaba.fluss.types.IntType);
    }

    @Test
    public void testNullHandling() {
        // Test null SeaTunnel type
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    visitor.visit("test_field", null);
                });

        // Test null column
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    visitor.visitColumn(null);
                });
    }

    @Test
    public void testRoundTripConversion() {
        // Test round-trip conversion: SeaTunnel → Fluss → SeaTunnel
        DecimalType originalDecimal = new DecimalType(20, 4);

        // Convert to Fluss
        DataType flussType = visitor.visit("test_field", originalDecimal);
        assertTrue(flussType instanceof com.alibaba.fluss.types.DecimalType);

        // Convert back to SeaTunnel
        org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> seaTunnelType =
                flussType.accept(FlussToSeaTunnelTypeVisitor.INSTANCE);
        assertTrue(seaTunnelType instanceof DecimalType);

        DecimalType resultDecimal = (DecimalType) seaTunnelType;
        assertEquals(originalDecimal.getPrecision(), resultDecimal.getPrecision());
        assertEquals(originalDecimal.getScale(), resultDecimal.getScale());
    }

    @Test
    public void testNestedComplexTypes() {
        // Test nested complex type: ARRAY<MAP<STRING, DECIMAL>>
        MapType<String, java.math.BigDecimal> innerMapType =
                new MapType<>(BasicType.STRING_TYPE, new DecimalType(10, 2));
        ArrayType<?, ?> nestedArrayType = ArrayType.of(innerMapType);

        DataType flussNestedType = visitor.visit("nested_field", nestedArrayType);
        assertTrue(flussNestedType instanceof com.alibaba.fluss.types.ArrayType);

        com.alibaba.fluss.types.ArrayType flussArray =
                (com.alibaba.fluss.types.ArrayType) flussNestedType;
        assertTrue(flussArray.getElementType() instanceof com.alibaba.fluss.types.MapType);

        com.alibaba.fluss.types.MapType flussMap =
                (com.alibaba.fluss.types.MapType) flussArray.getElementType();
        assertTrue(flussMap.getKeyType() instanceof com.alibaba.fluss.types.StringType);
        assertTrue(flussMap.getValueType() instanceof com.alibaba.fluss.types.DecimalType);

        com.alibaba.fluss.types.DecimalType flussDecimal =
                (com.alibaba.fluss.types.DecimalType) flussMap.getValueType();
        assertEquals(10, flussDecimal.getPrecision());
        assertEquals(2, flussDecimal.getScale());
    }
}
