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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public
/** Test cases for FlussTypeConverter to ensure all type conversions work correctly. */
static class FlussTypeConverterTest {

    @Test
    public void testBasicTypeConversions() {
        // Test BOOLEAN
        assertEquals(
                BasicType.BOOLEAN_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.BOOLEAN()));

        // Test TINYINT
        assertEquals(BasicType.BYTE_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.TINYINT()));

        // Test SMALLINT
        assertEquals(
                BasicType.SHORT_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.SMALLINT()));

        // Test INT
        assertEquals(BasicType.INT_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.INT()));

        // Test BIGINT
        assertEquals(BasicType.LONG_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.BIGINT()));

        // Test FLOAT
        assertEquals(BasicType.FLOAT_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.FLOAT()));

        // Test DOUBLE
        assertEquals(BasicType.DOUBLE_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.DOUBLE()));
    }

    @Test
    public void testStringTypeConversions() {
        // Test CHAR
        assertEquals(BasicType.STRING_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.CHAR(10)));

        // Test STRING
        assertEquals(BasicType.STRING_TYPE, FlussTypeConverter.toSeaTunnelType(DataTypes.STRING()));
    }

    @Test
    public void testDecimalTypeConversions() {
        // Test DECIMAL with different precision and scale
        DecimalType expectedDecimal1 = new DecimalType(10, 2);
        assertEquals(
                expectedDecimal1, FlussTypeConverter.toSeaTunnelType(DataTypes.DECIMAL(10, 2)));

        DecimalType expectedDecimal2 = new DecimalType(38, 18);
        assertEquals(
                expectedDecimal2, FlussTypeConverter.toSeaTunnelType(DataTypes.DECIMAL(38, 18)));
    }

    @Test
    public void testDateTimeTypeConversions() {
        // Test DATE
        assertEquals(
                LocalTimeType.LOCAL_DATE_TYPE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.DATE()));

        // Test TIME
        assertEquals(
                LocalTimeType.LOCAL_TIME_TYPE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.TIME()));

        // Test TIME with precision
        assertEquals(
                LocalTimeType.LOCAL_TIME_TYPE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.TIME(6)));

        // Test TIMESTAMP
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.TIMESTAMP()));

        // Test TIMESTAMP with precision
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.TIMESTAMP(9)));

        // Test TIMESTAMP_LTZ
        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.TIMESTAMP_LTZ()));

        // Test TIMESTAMP_LTZ with precision
        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.TIMESTAMP_LTZ(3)));
    }

    @Test
    public void testBinaryTypeConversions() {
        // Test BINARY
        assertEquals(
                PrimitiveByteArrayType.INSTANCE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.BINARY(100)));

        // Test BYTES
        assertEquals(
                PrimitiveByteArrayType.INSTANCE,
                FlussTypeConverter.toSeaTunnelType(DataTypes.BYTES()));
    }

    @Test
    public void testComplexTypeConversions() {
        // Test ARRAY
        DataType flussArrayType = DataTypes.ARRAY(DataTypes.STRING());
        SeaTunnelDataType<?> seaTunnelArrayType =
                FlussTypeConverter.toSeaTunnelType(flussArrayType);
        assertTrue(seaTunnelArrayType instanceof ArrayType);
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelArrayType;
        assertEquals(BasicType.STRING_TYPE, arrayType.getElementType());

        // Test MAP
        DataType flussMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        SeaTunnelDataType<?> seaTunnelMapType = FlussTypeConverter.toSeaTunnelType(flussMapType);
        assertTrue(seaTunnelMapType instanceof MapType);
        MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelMapType;
        assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        assertEquals(BasicType.INT_TYPE, mapType.getValueType());

        // Test ROW
        DataType flussRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT()));
        SeaTunnelDataType<?> seaTunnelRowType = FlussTypeConverter.toSeaTunnelType(flussRowType);
        assertTrue(seaTunnelRowType instanceof SeaTunnelRowType);
        SeaTunnelRowType rowType = (SeaTunnelRowType) seaTunnelRowType;
        assertEquals(3, rowType.getTotalFields());
        assertEquals("id", rowType.getFieldName(0));
        assertEquals("name", rowType.getFieldName(1));
        assertEquals("age", rowType.getFieldName(2));
        assertEquals(BasicType.LONG_TYPE, rowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(1));
        assertEquals(BasicType.INT_TYPE, rowType.getFieldType(2));
    }

    @Test
    public void testNestedComplexTypes() {
        // Test ARRAY of MAP
        DataType nestedType =
                DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.DECIMAL(10, 2)));
        SeaTunnelDataType<?> seaTunnelType = FlussTypeConverter.toSeaTunnelType(nestedType);
        assertTrue(seaTunnelType instanceof ArrayType);
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelType;
        assertTrue(arrayType.getElementType() instanceof MapType);
        MapType<?, ?> mapType = (MapType<?, ?>) arrayType.getElementType();
        assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        assertEquals(new DecimalType(10, 2), mapType.getValueType());
    }

    @Test
    public void testStringTypeConversionsFromString() {
        // Test legacy string-based conversion
        assertEquals(BasicType.BOOLEAN_TYPE, FlussTypeConverter.toSeaTunnelType("boolean"));
        assertEquals(BasicType.BYTE_TYPE, FlussTypeConverter.toSeaTunnelType("tinyint"));
        assertEquals(BasicType.SHORT_TYPE, FlussTypeConverter.toSeaTunnelType("smallint"));
        assertEquals(BasicType.INT_TYPE, FlussTypeConverter.toSeaTunnelType("int"));
        assertEquals(BasicType.LONG_TYPE, FlussTypeConverter.toSeaTunnelType("bigint"));
        assertEquals(BasicType.FLOAT_TYPE, FlussTypeConverter.toSeaTunnelType("float"));
        assertEquals(BasicType.DOUBLE_TYPE, FlussTypeConverter.toSeaTunnelType("double"));
        assertEquals(BasicType.STRING_TYPE, FlussTypeConverter.toSeaTunnelType("string"));
        assertEquals(PrimitiveByteArrayType.INSTANCE, FlussTypeConverter.toSeaTunnelType("binary"));
        assertEquals(LocalTimeType.LOCAL_DATE_TYPE, FlussTypeConverter.toSeaTunnelType("date"));
        assertEquals(LocalTimeType.LOCAL_TIME_TYPE, FlussTypeConverter.toSeaTunnelType("time"));
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                FlussTypeConverter.toSeaTunnelType("timestamp"));
    }

    @Test
    public void testDecimalStringConversion() {
        // Test decimal string parsing
        DecimalType expectedDecimal = new DecimalType(10, 2);
        assertEquals(expectedDecimal, FlussTypeConverter.toSeaTunnelType("decimal(10,2)"));
        assertEquals(expectedDecimal, FlussTypeConverter.toSeaTunnelType("DECIMAL(10,2)"));
        assertEquals(expectedDecimal, FlussTypeConverter.toSeaTunnelType("decimal(10, 2)"));
    }

    @Test
    public void testSeaTunnelToFlussTypeConversion() {
        // Test reverse conversion
        assertEquals("BOOLEAN", FlussTypeConverter.toFlussType(BasicType.BOOLEAN_TYPE));
        assertEquals("TINYINT", FlussTypeConverter.toFlussType(BasicType.BYTE_TYPE));
        assertEquals("SMALLINT", FlussTypeConverter.toFlussType(BasicType.SHORT_TYPE));
        assertEquals("INT", FlussTypeConverter.toFlussType(BasicType.INT_TYPE));
        assertEquals("BIGINT", FlussTypeConverter.toFlussType(BasicType.LONG_TYPE));
        assertEquals("FLOAT", FlussTypeConverter.toFlussType(BasicType.FLOAT_TYPE));
        assertEquals("DOUBLE", FlussTypeConverter.toFlussType(BasicType.DOUBLE_TYPE));
        assertEquals("STRING", FlussTypeConverter.toFlussType(BasicType.STRING_TYPE));
        assertEquals("BINARY", FlussTypeConverter.toFlussType(PrimitiveByteArrayType.INSTANCE));
        assertEquals("DATE", FlussTypeConverter.toFlussType(LocalTimeType.LOCAL_DATE_TYPE));
        assertEquals("TIME", FlussTypeConverter.toFlussType(LocalTimeType.LOCAL_TIME_TYPE));
        assertEquals(
                "TIMESTAMP", FlussTypeConverter.toFlussType(LocalTimeType.LOCAL_DATE_TIME_TYPE));

        // Test decimal conversion
        DecimalType decimalType = new DecimalType(10, 2);
        assertEquals("DECIMAL(10,2)", FlussTypeConverter.toFlussType(decimalType));
    }

    @Test
    public void testTableDescriptorConversion() {
        // Create a mock TableDescriptor
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.BIGINT(), false, "Primary key");
        schemaBuilder.column("name", DataTypes.STRING(), true, "User name");
        schemaBuilder.column("age", DataTypes.INT(), true, "User age");
        schemaBuilder.column("balance", DataTypes.DECIMAL(10, 2), true, "Account balance");
        schemaBuilder.column("created_at", DataTypes.TIMESTAMP(), true, "Creation time");
        schemaBuilder.primaryKey("id");

        Schema schema = schemaBuilder.build();

        Map<String, String> properties = new HashMap<>();
        properties.put("bucket.num", "4");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .properties(properties)
                        .comment("Test table")
                        .build();

        // Convert to SeaTunnel CatalogTable
        CatalogTable catalogTable = FlussTypeConverter.toSeaTunnelTable(tableDescriptor);

        // Verify the conversion
        assertNotNull(catalogTable);
        assertEquals("Test table", catalogTable.getComment());

        TableSchema tableSchema = catalogTable.getTableSchema();
        assertNotNull(tableSchema);

        List<Column> columns = tableSchema.getColumns();
        assertEquals(5, columns.size());

        // Verify columns
        Column idColumn = columns.get(0);
        assertEquals("id", idColumn.getName());
        assertEquals(BasicType.LONG_TYPE, idColumn.getDataType());
        assertEquals("Primary key", idColumn.getComment());

        Column nameColumn = columns.get(1);
        assertEquals("name", nameColumn.getName());
        assertEquals(BasicType.STRING_TYPE, nameColumn.getDataType());
        assertEquals("User name", nameColumn.getComment());

        Column balanceColumn = columns.get(3);
        assertEquals("balance", balanceColumn.getName());
        assertTrue(balanceColumn.getDataType() instanceof DecimalType);
        DecimalType balanceDecimal = (DecimalType) balanceColumn.getDataType();
        assertEquals(10, balanceDecimal.getPrecision());
        assertEquals(2, balanceDecimal.getScale());

        Column timestampColumn = columns.get(4);
        assertEquals("created_at", timestampColumn.getName());
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, timestampColumn.getDataType());

        // Verify primary key
        assertNotNull(tableSchema.getPrimaryKey());
        assertEquals(1, tableSchema.getPrimaryKey().getColumnNames().size());
        assertEquals("id", tableSchema.getPrimaryKey().getColumnNames().get(0));

        // Verify options
        Map<String, String> options = catalogTable.getOptions();
        assertTrue(options.containsKey("connector"));
        assertEquals("fluss", options.get("connector"));
        assertTrue(options.containsKey("bucket.num"));
        assertEquals("4", options.get("bucket.num"));
    }

    @Test
    public void testTableInfoConversion() {
        // Create a mock TableInfo
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.BIGINT());
        schemaBuilder.column("data", DataTypes.STRING());
        Schema schema = schemaBuilder.build();

        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        TableInfo tableInfo =
                new TableInfo(
                        com.alibaba.fluss.metadata.TablePath.of("test_db", "test_table"),
                        1L, // table id
                        tableDescriptor);

        // Convert to SeaTunnel CatalogTable
        CatalogTable catalogTable = FlussTypeConverter.toSeaTunnelTable(tableInfo);

        // Verify the conversion
        assertNotNull(catalogTable);
        TableSchema tableSchema = catalogTable.getTableSchema();
        assertNotNull(tableSchema);

        List<Column> columns = tableSchema.getColumns();
        assertEquals(2, columns.size());

        assertEquals("id", columns.get(0).getName());
        assertEquals(BasicType.LONG_TYPE, columns.get(0).getDataType());

        assertEquals("data", columns.get(1).getName());
        assertEquals(BasicType.STRING_TYPE, columns.get(1).getDataType());
    }

    @Test
    public void testUnsupportedTypeHandling() {
        // Test unsupported type string
        SeaTunnelDataType<?> result = FlussTypeConverter.toSeaTunnelType("unsupported_type");
        assertEquals(BasicType.STRING_TYPE, result); // Should fallback to STRING
    }

    @Test
    public void testNullInputHandling() {
        // Test null inputs
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    FlussTypeConverter.toSeaTunnelType((DataType) null);
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    FlussTypeConverter.toSeaTunnelType((String) null);
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    FlussTypeConverter.toFlussType(null);
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    FlussTypeConverter.toSeaTunnelTable((TableDescriptor) null);
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    FlussTypeConverter.toSeaTunnelTable((TableInfo) null);
                });
    }
}
