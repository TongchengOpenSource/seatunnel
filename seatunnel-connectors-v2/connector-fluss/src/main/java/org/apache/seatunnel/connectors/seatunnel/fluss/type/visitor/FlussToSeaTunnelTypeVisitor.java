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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataTypeVisitor;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;

public class FlussToSeaTunnelTypeVisitor implements DataTypeVisitor<SeaTunnelDataType<?>> {

    public static final FlussToSeaTunnelTypeVisitor INSTANCE = new FlussToSeaTunnelTypeVisitor();

    @Override
    public SeaTunnelDataType<?> visit(BooleanType booleanType) {
        return BasicType.BOOLEAN_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(TinyIntType tinyIntType) {
        return BasicType.BYTE_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(SmallIntType smallIntType) {
        return BasicType.SHORT_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(IntType intType) {
        return BasicType.INT_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(BigIntType bigIntType) {
        return BasicType.LONG_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(FloatType floatType) {
        return BasicType.FLOAT_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(DoubleType doubleType) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(StringType stringType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(CharType charType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(BytesType bytesType) {
        return PrimitiveByteArrayType.INSTANCE;
    }

    @Override
    public SeaTunnelDataType<?> visit(BinaryType binaryType) {
        return PrimitiveByteArrayType.INSTANCE;
    }

    @Override
    public SeaTunnelDataType<?> visit(DateType dateType) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(TimeType timeType) {
        return LocalTimeType.LOCAL_TIME_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(TimestampType timestampType) {
        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(LocalZonedTimestampType localZonedTimestampType) {
        return LocalTimeType.OFFSET_DATE_TIME_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> visit(com.alibaba.fluss.types.DecimalType decimalType) {
        return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public SeaTunnelDataType<?> visit(ArrayType arrayType) {
        SeaTunnelDataType<?> elementType = arrayType.getElementType().accept(this);
        return org.apache.seatunnel.api.table.type.ArrayType.of(elementType);
    }

    @Override
    public SeaTunnelDataType<?> visit(MapType mapType) {
        SeaTunnelDataType<?> keyType = mapType.getKeyType().accept(this);
        SeaTunnelDataType<?> valueType = mapType.getValueType().accept(this);
        return new org.apache.seatunnel.api.table.type.MapType<>(keyType, valueType);
    }

    @Override
    public SeaTunnelDataType<?> visit(RowType rowType) {
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        SeaTunnelDataType<?>[] fieldTypes =
                rowType.getFields().stream()
                        .map(field -> field.getType().accept(this))
                        .toArray(SeaTunnelDataType<?>[]::new);
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }
}
