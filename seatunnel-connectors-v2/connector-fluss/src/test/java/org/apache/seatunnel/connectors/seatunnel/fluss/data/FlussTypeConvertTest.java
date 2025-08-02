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

package org.apache.seatunnel.connectors.seatunnel.fluss.data;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.fluss.type.FlussTypeConvert;

import org.junit.jupiter.api.Test;

import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;

public
/** Test cases for FlussTypeMapper to ensure the type mapper works correctly. */
static class FlussTypeConvertTest {

    private final FlussTypeConvert typeMapper = FlussTypeConvert.INSTANCE;

    @Test
    public void testIdentifier() {
        assertEquals("Fluss", typeMapper.identifier());
    }

    @Test
    public void testConvertBasicTypes() {
        // Test BOOLEAN
        BasicTypeDefine<DataType> booleanDefine =
                createTypeDefine("test_boolean", DataTypes.BOOLEAN(), "BOOLEAN");
        Column booleanColumn = typeMapper.convert(booleanDefine);
        assertEquals("test_boolean", booleanColumn.getName());
        assertEquals(BasicType.BOOLEAN_TYPE, booleanColumn.getDataType());
        assertTrue(booleanColumn.isNullable());

        // Test INT
        BasicTypeDefine<DataType> intDefine = createTypeDefine("test_int", DataTypes.INT(), "INT");
        Column intColumn = typeMapper.convert(intDefine);
        assertEquals("test_int", intColumn.getName());
        assertEquals(BasicType.INT_TYPE, intColumn.getDataType());

        // Test BIGINT
        BasicTypeDefine<DataType> bigintDefine =
                createTypeDefine("test_bigint", DataTypes.BIGINT(), "BIGINT");
        Column bigintColumn = typeMapper.convert(bigintDefine);
        assertEquals("test_bigint", bigintColumn.getName());
        assertEquals(BasicType.LONG_TYPE, bigintColumn.getDataType());

        // Test STRING
        BasicTypeDefine<DataType> stringDefine =
                createTypeDefine("test_string", DataTypes.STRING(), "STRING");
        Column stringColumn = typeMapper.convert(stringDefine);
        assertEquals("test_string", stringColumn.getName());
        assertEquals(BasicType.STRING_TYPE, stringColumn.getDataType());
    }

    @Test
    public void testConvertDecimalType() {
        BasicTypeDefine<DataType> decimalDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_decimal")
                        .nativeType(DataTypes.DECIMAL(10, 2))
                        .dataType("DECIMAL")
                        .columnType("DECIMAL(10,2)")
                        .precision(10L)
                        .scale(2)
                        .nullable(true)
                        .comment("Test decimal field")
                        .build();

        Column decimalColumn = typeMapper.convert(decimalDefine);
        assertEquals("test_decimal", decimalColumn.getName());
        assertTrue(decimalColumn.getDataType() instanceof DecimalType);
        DecimalType decimalType = (DecimalType) decimalColumn.getDataType();
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
        assertEquals("Test decimal field", decimalColumn.getComment());
    }

    @Test
    public void testConvertDateTimeTypes() {
        // Test DATE
        BasicTypeDefine<DataType> dateDefine =
                createTypeDefine("test_date", DataTypes.DATE(), "DATE");
        Column dateColumn = typeMapper.convert(dateDefine);
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

        Column timeColumn = typeMapper.convert(timeDefine);
        assertEquals(LocalTimeType.LOCAL_TIME_TYPE, timeColumn.getDataType());
        assertEquals(6, timeColumn.getScale());

        // Test TIMESTAMP
        BasicTypeDefine<DataType> timestampDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_timestamp")
                        .nativeType(DataTypes.TIMESTAMP(9))
                        .dataType("TIMESTAMP")
                        .columnType("TIMESTAMP(9)")
                        .scale(9)
                        .nullable(true)
                        .build();

        Column timestampColumn = typeMapper.convert(timestampDefine);
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, timestampColumn.getDataType());
        assertEquals(9, timestampColumn.getScale());
    }

    @Test
    public void testReconvertBasicTypes() {
        // Test BOOLEAN reconvert
        Column booleanColumn =
                PhysicalColumn.of(
                        "test_boolean", BasicType.BOOLEAN_TYPE, 0L, true, null, "Test boolean");
        BasicTypeDefine<DataType> booleanDefine = typeMapper.reconvert(booleanColumn);
        assertEquals("test_boolean", booleanDefine.getName());
        assertTrue(booleanDefine.getNativeType() instanceof com.alibaba.fluss.types.BooleanType);
        assertEquals("BOOLEAN", booleanDefine.getDataType());
        assertEquals("BOOLEAN", booleanDefine.getColumnType());
        assertTrue(booleanDefine.isNullable());
        assertEquals("Test boolean", booleanDefine.getComment());

        // Test INT reconvert
        Column intColumn =
                PhysicalColumn.of("test_int", BasicType.INT_TYPE, 0L, false, 42, "Test int");
        BasicTypeDefine<DataType> intDefine = typeMapper.reconvert(intColumn);
        assertEquals("test_int", intDefine.getName());
        assertTrue(intDefine.getNativeType() instanceof com.alibaba.fluss.types.IntType);
        assertEquals("INT", intDefine.getDataType());
        assertEquals("INT", intDefine.getColumnType());
        assertFalse(intDefine.isNullable());
        assertEquals(42, intDefine.getDefaultValue());
        assertEquals("Test int", intDefine.getComment());

        // Test STRING reconvert
        Column stringColumn =
                PhysicalColumn.of(
                        "test_string", BasicType.STRING_TYPE, 0L, true, "default", "Test string");
        BasicTypeDefine<DataType> stringDefine = typeMapper.reconvert(stringColumn);
        assertEquals("test_string", stringDefine.getName());
        assertTrue(stringDefine.getNativeType() instanceof com.alibaba.fluss.types.StringType);
        assertEquals("STRING", stringDefine.getDataType());
        assertEquals("STRING", stringDefine.getColumnType());
        assertEquals("default", stringDefine.getDefaultValue());
    }

    @Test
    public void testReconvertDecimalType() {
        Column decimalColumn =
                PhysicalColumn.builder()
                        .name("test_decimal")
                        .dataType(new DecimalType(15, 3))
                        .columnLength(15L)
                        .scale(3)
                        .nullable(true)
                        .defaultValue(null)
                        .comment("Test decimal")
                        .build();

        BasicTypeDefine<DataType> decimalDefine = typeMapper.reconvert(decimalColumn);
        assertEquals("test_decimal", decimalDefine.getName());
        assertTrue(decimalDefine.getNativeType() instanceof com.alibaba.fluss.types.DecimalType);
        assertEquals("DECIMAL", decimalDefine.getDataType());
        assertEquals("DECIMAL(15,3)", decimalDefine.getColumnType());
        assertEquals(15L, decimalDefine.getPrecision());
        assertEquals(3, decimalDefine.getScale());
        assertTrue(decimalDefine.isNullable());
        assertEquals("Test decimal", decimalDefine.getComment());
    }

    @Test
    public void testReconvertStringWithLength() {
        // Test STRING with length (should convert to CHAR)
        Column charColumn =
                PhysicalColumn.builder()
                        .name("test_char")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .nullable(false)
                        .comment("Test char field")
                        .build();

        BasicTypeDefine<DataType> charDefine = typeMapper.reconvert(charColumn);
        assertEquals("test_char", charDefine.getName());
        assertTrue(charDefine.getNativeType() instanceof com.alibaba.fluss.types.CharType);
        assertEquals("CHAR", charDefine.getDataType());
        assertEquals("CHAR(255)", charDefine.getColumnType());
        assertEquals(255L, charDefine.getLength());
        assertFalse(charDefine.isNullable());
        assertEquals("Test char field", charDefine.getComment());
    }

    @Test
    public void testReconvertDateTimeTypes() {
        // Test DATE reconvert
        Column dateColumn =
                PhysicalColumn.of(
                        "test_date", LocalTimeType.LOCAL_DATE_TYPE, 0L, true, null, "Test date");
        BasicTypeDefine<DataType> dateDefine = typeMapper.reconvert(dateColumn);
        assertEquals("test_date", dateDefine.getName());
        assertTrue(dateDefine.getNativeType() instanceof com.alibaba.fluss.types.DateType);
        assertEquals("DATE", dateDefine.getDataType());
        assertEquals("DATE", dateDefine.getColumnType());

        // Test TIME reconvert with scale
        Column timeColumn =
                PhysicalColumn.builder()
                        .name("test_time")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .nullable(true)
                        .comment("Test time")
                        .build();

        BasicTypeDefine<DataType> timeDefine = typeMapper.reconvert(timeColumn);
        assertEquals("test_time", timeDefine.getName());
        assertTrue(timeDefine.getNativeType() instanceof com.alibaba.fluss.types.TimeType);
        assertEquals("TIME", timeDefine.getDataType());
        assertEquals("TIME(3)", timeDefine.getColumnType());
        assertEquals(3, timeDefine.getScale());

        // Test TIMESTAMP reconvert with scale
        Column timestampColumn =
                PhysicalColumn.builder()
                        .name("test_timestamp")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(6)
                        .nullable(true)
                        .comment("Test timestamp")
                        .build();

        BasicTypeDefine<DataType> timestampDefine = typeMapper.reconvert(timestampColumn);
        assertEquals("test_timestamp", timestampDefine.getName());
        assertTrue(
                timestampDefine.getNativeType() instanceof com.alibaba.fluss.types.TimestampType);
        assertEquals("TIMESTAMP", timestampDefine.getDataType());
        assertEquals("TIMESTAMP(6)", timestampDefine.getColumnType());
        assertEquals(6, timestampDefine.getScale());

        // Test TIMESTAMP_LTZ reconvert
        Column timestampLtzColumn =
                PhysicalColumn.builder()
                        .name("test_timestamp_ltz")
                        .dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE)
                        .scale(9)
                        .nullable(true)
                        .comment("Test timestamp with timezone")
                        .build();

        BasicTypeDefine<DataType> timestampLtzDefine = typeMapper.reconvert(timestampLtzColumn);
        assertEquals("test_timestamp_ltz", timestampLtzDefine.getName());
        assertTrue(
                timestampLtzDefine.getNativeType()
                        instanceof com.alibaba.fluss.types.LocalZonedTimestampType);
        assertEquals("TIMESTAMP_LTZ", timestampLtzDefine.getDataType());
        assertEquals("TIMESTAMP_LTZ(9)", timestampLtzDefine.getColumnType());
        assertEquals(9, timestampLtzDefine.getScale());
    }

    @Test
    public void testRoundTripConversion() {
        // Test round-trip conversion: Fluss -> SeaTunnel -> Fluss

        // Original Fluss type definition
        BasicTypeDefine<DataType> originalDefine =
                BasicTypeDefine.<DataType>builder()
                        .name("test_field")
                        .nativeType(DataTypes.DECIMAL(20, 4))
                        .dataType("DECIMAL")
                        .columnType("DECIMAL(20,4)")
                        .precision(20L)
                        .scale(4)
                        .nullable(false)
                        .comment("Test round-trip conversion")
                        .build();

        // Convert to SeaTunnel Column
        Column seaTunnelColumn = typeMapper.convert(originalDefine);

        // Convert back to Fluss BasicTypeDefine
        BasicTypeDefine<DataType> reconvertedDefine = typeMapper.reconvert(seaTunnelColumn);

        // Verify the round-trip conversion
        assertEquals(originalDefine.getName(), reconvertedDefine.getName());
        assertTrue(
                reconvertedDefine.getNativeType() instanceof com.alibaba.fluss.types.DecimalType);
        assertEquals(originalDefine.getDataType(), reconvertedDefine.getDataType());
        assertEquals(originalDefine.getColumnType(), reconvertedDefine.getColumnType());
        assertEquals(originalDefine.getPrecision(), reconvertedDefine.getPrecision());
        assertEquals(originalDefine.getScale(), reconvertedDefine.getScale());
        assertEquals(originalDefine.isNullable(), reconvertedDefine.isNullable());
        assertEquals(originalDefine.getComment(), reconvertedDefine.getComment());
    }

    @Test
    public void testTypeMapperInstance() {
        // Test that INSTANCE is properly initialized
        assertNotNull(FlussTypeConvert.INSTANCE);
        assertEquals("Fluss", FlussTypeConvert.INSTANCE.identifier());

        // Test that multiple calls return the same instance
        assertSame(FlussTypeConvert.INSTANCE, FlussTypeConvert.INSTANCE);
    }

    @Test
    public void testNullHandling() {
        // Test null handling in convert method
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    typeMapper.convert(null);
                });

        // Test null handling in reconvert method
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    typeMapper.reconvert(null);
                });

        // Test null handling in list convert method
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    typeMapper.convert((List<BasicTypeDefine<DataType>>) null);
                });

        // Test null handling in list reconvert method
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    typeMapper.reconvert((List<Column>) null);
                });
    }

    @Test
    public void testComplexTypeConversion() {
        // Test ARRAY type
        BasicTypeDefine<DataType> arrayDefine =
                createTypeDefine(
                        "test_array", DataTypes.ARRAY(DataTypes.STRING()), "ARRAY<STRING>");
        Column arrayColumn = typeMapper.convert(arrayDefine);
        assertEquals("test_array", arrayColumn.getName());
        assertTrue(
                arrayColumn.getDataType() instanceof org.apache.seatunnel.api.table.type.ArrayType);

        // Test MAP type
        BasicTypeDefine<DataType> mapDefine =
                createTypeDefine(
                        "test_map",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        "MAP<STRING,INT>");
        Column mapColumn = typeMapper.convert(mapDefine);
        assertEquals("test_map", mapColumn.getName());
        assertTrue(mapColumn.getDataType() instanceof org.apache.seatunnel.api.table.type.MapType);

        // Test ROW type
        BasicTypeDefine<DataType> rowDefine =
                createTypeDefine(
                        "test_row",
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("name", DataTypes.STRING())),
                        "ROW<id BIGINT, name STRING>");
        Column rowColumn = typeMapper.convert(rowDefine);
        assertEquals("test_row", rowColumn.getName());
        assertTrue(
                rowColumn.getDataType()
                        instanceof org.apache.seatunnel.api.table.type.SeaTunnelRowType);
    }

    @Test
    public void testListConversions() {
        // Test list convert functionality
        List<BasicTypeDefine<DataType>> typeDefines =
                Arrays.asList(
                        createTypeDefine("field1", DataTypes.BOOLEAN(), "BOOLEAN"),
                        createTypeDefine("field2", DataTypes.INT(), "INT"),
                        createTypeDefine("field3", DataTypes.STRING(), "STRING"));

        List<Column> columns = typeMapper.convert(typeDefines);
        assertEquals(3, columns.size());
        assertEquals("field1", columns.get(0).getName());
        assertEquals("field2", columns.get(1).getName());
        assertEquals("field3", columns.get(2).getName());

        // Test list reconvert functionality
        List<BasicTypeDefine<DataType>> reconvertedDefines = typeMapper.reconvert(columns);
        assertEquals(3, reconvertedDefines.size());
        assertEquals("field1", reconvertedDefines.get(0).getName());
        assertEquals("field2", reconvertedDefines.get(1).getName());
        assertEquals("field3", reconvertedDefines.get(2).getName());
    }

    @Test
    public void testTypeConverterInterface() {
        // Test that FlussTypeMapper properly implements TypeConverter interface
        assertTrue(typeMapper instanceof TypeConverter);

        // Test serializable (TypeConverter extends Serializable)
        assertTrue(typeMapper instanceof java.io.Serializable);

        // Test identifier method
        assertNotNull(typeMapper.identifier());
        assertFalse(typeMapper.identifier().isEmpty());

        // Test toString method
        String toString = typeMapper.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("FlussTypeMapper"));
        assertTrue(toString.contains("Fluss"));
    }

    @Test
    public void testAutoServiceRegistration() {
        // Test that the type mapper is properly registered via @AutoService
        // This would be tested by the ServiceLoader in a real environment

        // Verify the annotation is present
        AutoService autoService = FlussTypeConvert.class.getAnnotation(AutoService.class);
        assertNotNull(autoService);
        assertEquals(TypeConverter.class, autoService.value()[0]);
    }

    private BasicTypeDefine<DataType> createTypeDefine(
            String name, DataType nativeType, String dataType) {
        return BasicTypeDefine.<DataType>builder()
                .name(name)
                .nativeType(nativeType)
                .dataType(dataType)
                .columnType(dataType)
                .nullable(true)
                .comment("Test field")
                .build();
    }
}
