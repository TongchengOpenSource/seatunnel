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

package org.apache.seatunnel.connectors.seatunnel.fluss.type.visitor;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;

import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class SeaTunnelToFlussTypeVisitor {

    public static final SeaTunnelToFlussTypeVisitor INSTANCE = new SeaTunnelToFlussTypeVisitor();

    private SeaTunnelToFlussTypeVisitor() {}

    public BasicTypeDefine<DataType> visitColumn(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder<DataType> builder =
                BasicTypeDefine.<DataType>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        SeaTunnelDataType<?> dataType = column.getDataType();
        Integer scale = column.getScale();
        Long columnLength = column.getColumnLength();

        switch (dataType.getSqlType()) {
            case BOOLEAN:
                BooleanType booleanType = DataTypes.BOOLEAN();
                builder.nativeType(booleanType);
                builder.dataType("BOOLEAN");
                builder.columnType("BOOLEAN");
                break;
            case TINYINT:
                TinyIntType tinyIntType = DataTypes.TINYINT();
                builder.nativeType(tinyIntType);
                builder.dataType("TINYINT");
                builder.columnType("TINYINT");
                break;
            case SMALLINT:
                SmallIntType smallIntType = DataTypes.SMALLINT();
                builder.nativeType(smallIntType);
                builder.dataType("SMALLINT");
                builder.columnType("SMALLINT");
                break;
            case INT:
                IntType intType = DataTypes.INT();
                builder.nativeType(intType);
                builder.dataType("INT");
                builder.columnType("INT");
                break;
            case BIGINT:
                BigIntType bigIntType = DataTypes.BIGINT();
                builder.nativeType(bigIntType);
                builder.dataType("BIGINT");
                builder.columnType("BIGINT");
                break;
            case FLOAT:
                FloatType floatType = DataTypes.FLOAT();
                builder.nativeType(floatType);
                builder.dataType("FLOAT");
                builder.columnType("FLOAT");
                break;
            case DOUBLE:
                DoubleType doubleType = DataTypes.DOUBLE();
                builder.nativeType(doubleType);
                builder.dataType("DOUBLE");
                builder.columnType("DOUBLE");
                break;
            case STRING:
                if (columnLength != null && columnLength > 0 && columnLength <= Integer.MAX_VALUE) {
                    CharType charType = DataTypes.CHAR(columnLength.intValue());
                    builder.nativeType(charType);
                    builder.dataType("CHAR");
                    builder.columnType(String.format("CHAR(%d)", columnLength));
                    builder.length(columnLength);
                } else {
                    StringType stringType = DataTypes.STRING();
                    builder.nativeType(stringType);
                    builder.dataType("STRING");
                    builder.columnType("STRING");
                }
                break;
            case DECIMAL:
                DecimalType seaTunnelDecimalType = (DecimalType) dataType;
                com.alibaba.fluss.types.DecimalType flussDecimalType =
                        DataTypes.DECIMAL(
                                seaTunnelDecimalType.getPrecision(),
                                seaTunnelDecimalType.getScale());
                builder.nativeType(flussDecimalType);
                builder.dataType("DECIMAL");
                builder.columnType(
                        String.format(
                                "DECIMAL(%d,%d)",
                                seaTunnelDecimalType.getPrecision(),
                                seaTunnelDecimalType.getScale()));
                builder.precision((long) seaTunnelDecimalType.getPrecision());
                builder.scale(seaTunnelDecimalType.getScale());
                break;
            case DATE:
                DateType dateType = DataTypes.DATE();
                builder.nativeType(dateType);
                builder.dataType("DATE");
                builder.columnType("DATE");
                break;
            case TIME:
                int timePrecision = Objects.isNull(scale) ? TimeType.DEFAULT_PRECISION : scale;
                TimeType timeType = DataTypes.TIME(timePrecision);
                builder.nativeType(timeType);
                builder.dataType("TIME");
                builder.columnType(
                        timePrecision == TimeType.DEFAULT_PRECISION
                                ? "TIME"
                                : String.format("TIME(%d)", timePrecision));
                builder.scale(timePrecision);
                break;
            case TIMESTAMP:
                int timestampPrecision =
                        Objects.isNull(scale) ? TimestampType.DEFAULT_PRECISION : scale;
                TimestampType timestampType = DataTypes.TIMESTAMP(timestampPrecision);
                builder.nativeType(timestampType);
                builder.dataType("TIMESTAMP");
                builder.columnType(
                        timestampPrecision == TimestampType.DEFAULT_PRECISION
                                ? "TIMESTAMP"
                                : String.format("TIMESTAMP(%d)", timestampPrecision));
                builder.scale(timestampPrecision);
                break;
            case TIMESTAMP_TZ:
                int timestampLtzPrecision =
                        Objects.isNull(scale) ? LocalZonedTimestampType.DEFAULT_PRECISION : scale;
                LocalZonedTimestampType timestampLtzType =
                        DataTypes.TIMESTAMP_LTZ(timestampLtzPrecision);
                builder.nativeType(timestampLtzType);
                builder.dataType("TIMESTAMP_LTZ");
                builder.columnType(
                        timestampLtzPrecision == LocalZonedTimestampType.DEFAULT_PRECISION
                                ? "TIMESTAMP_LTZ"
                                : String.format("TIMESTAMP_LTZ(%d)", timestampLtzPrecision));
                builder.scale(timestampLtzPrecision);
                break;
            case BYTES:
                if (columnLength != null && columnLength > 0 && columnLength <= Integer.MAX_VALUE) {
                    BinaryType binaryType = DataTypes.BINARY(columnLength.intValue());
                    builder.nativeType(binaryType);
                    builder.dataType("BINARY");
                    builder.columnType(String.format("BINARY(%d)", columnLength));
                    builder.length(columnLength);
                } else {
                    BytesType bytesType = DataTypes.BYTES();
                    builder.nativeType(bytesType);
                    builder.dataType("BYTES");
                    builder.columnType("BYTES");
                }
                break;
            default:
                throw CommonError.unsupportedDataType(
                        "fluss", dataType.getSqlType().toString(), column.getName());
        }

        return builder.build();
    }

    public DataType visit(String fieldName, SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case STRING:
                return DataTypes.STRING();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case TIMESTAMP_TZ:
                return DataTypes.TIMESTAMP_LTZ();
            case BYTES:
                return DataTypes.BYTES();
            case ARRAY:
                org.apache.seatunnel.api.table.type.ArrayType<?, ?> arrayType =
                        (org.apache.seatunnel.api.table.type.ArrayType<?, ?>) dataType;
                DataType elementType = visit(fieldName + "_element", arrayType.getElementType());
                return DataTypes.ARRAY(elementType);
            case MAP:
                org.apache.seatunnel.api.table.type.MapType<?, ?> mapType =
                        (org.apache.seatunnel.api.table.type.MapType<?, ?>) dataType;
                DataType keyType = visit(fieldName + "_key", mapType.getKeyType());
                DataType valueType = visit(fieldName + "_value", mapType.getValueType());
                return DataTypes.MAP(keyType, valueType);
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                com.alibaba.fluss.types.DataField[] fields =
                        new com.alibaba.fluss.types.DataField[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    String fieldName1 = rowType.getFieldName(i);
                    DataType fieldType = visit(fieldName1, rowType.getFieldType(i));
                    fields[i] = DataTypes.FIELD(fieldName1, fieldType);
                }
                return DataTypes.ROW(fields);
            default:
                throw CommonError.unsupportedDataType(
                        "fluss", dataType.getSqlType().toString(), fieldName);
        }
    }
}
