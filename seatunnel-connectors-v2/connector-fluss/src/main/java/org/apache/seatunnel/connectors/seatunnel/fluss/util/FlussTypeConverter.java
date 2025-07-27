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

package org.apache.seatunnel.connectors.seatunnel.fluss.util;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fluss.exception.FlussConnectorException;

import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.types.BinaryString;
import com.alibaba.fluss.utils.types.Decimal;
import com.alibaba.fluss.utils.types.TimestampNtz;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class FlussTypeConverter {

    /** Convert SeaTunnelRow to Fluss GenericRow */
    public static GenericRow convertToFlussRow(
            SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
        if (seaTunnelRow == null) {
            return null;
        }

        GenericRow flussRow = new GenericRow(rowType.getTotalFields());
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Object value = seaTunnelRow.getField(i);
            Object convertedValue = convertToFlussType(rowType.getFieldType(i), value);
            flussRow.setField(i, convertedValue);
        }
        return flussRow;
    }

    /** Convert Fluss InternalRow to SeaTunnelRow */
    public static SeaTunnelRow convertFromFlussRow(InternalRow flussRow, SeaTunnelRowType rowType) {
        if (flussRow == null) {
            return null;
        }

        Object[] values = new Object[rowType.getTotalFields()];
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Object value =
                    flussRow.isNullAt(i)
                            ? null
                            : getFieldValue(flussRow, i, rowType.getFieldType(i));
            values[i] = convertFromFlussType(rowType.getFieldType(i), value);
        }
        return new SeaTunnelRow(values);
    }

    /** Convert SeaTunnel data type to Fluss data type */
    public static Object convertToFlussType(SeaTunnelDataType<?> seaTunnelType, Object value) {
        if (value == null) {
            return null;
        }

        try {
            switch (seaTunnelType.getSqlType()) {
                case BOOLEAN:
                    return value;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    return value;
                case FLOAT:
                case DOUBLE:
                    return value;
                case DECIMAL:
                    if (value instanceof BigDecimal) {
                        return value;
                    }
                    return new BigDecimal(value.toString());
                case STRING:
                    return BinaryString.fromString(value.toString());
                case BYTES:
                    return value;
                case DATE:
                    if (value instanceof LocalDate) {
                        return (int) ((LocalDate) value).toEpochDay();
                    }
                    break;
                case TIME:
                    if (value instanceof LocalTime) {
                        return ((LocalTime) value).toNanoOfDay() / 1000; // Convert to microseconds
                    }
                    break;
                case TIMESTAMP:
                    if (value instanceof LocalDateTime) {
                        return TimestampNtz.fromLocalDateTime((LocalDateTime) value);
                    }
                    break;
                case ARRAY:
                    return convertArrayToFluss((ArrayType<?, ?>) seaTunnelType, value);
                case MAP:
                    return convertMapToFluss((MapType<?, ?>) seaTunnelType, value);
                case ROW:
                    return convertRowToFluss(
                            (SeaTunnelRowType) seaTunnelType, (SeaTunnelRow) value);
                default:
                    throw new FlussConnectorException(
                            FlussConnectorErrorCode.UNSUPPORTED_OPERATION,
                            "Unsupported SeaTunnel data type: " + seaTunnelType.getSqlType());
            }
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.SERIALIZATION_FAILED,
                    "Failed to convert SeaTunnel type to Fluss type",
                    e);
        }

        return value;
    }

    /** Convert Fluss data type to SeaTunnel data type */
    public static Object convertFromFlussType(
            SeaTunnelDataType<?> seaTunnelType, Object flussValue) {
        if (flussValue == null) {
            return null;
        }

        try {
            switch (seaTunnelType.getSqlType()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case STRING:
                    if (flussValue instanceof BinaryString) {
                        return ((BinaryString) flussValue).toString();
                    }
                    return flussValue.toString();
                case BYTES:
                    return flussValue;
                case DATE:
                    if (flussValue instanceof Integer) {
                        return LocalDate.ofEpochDay((Integer) flussValue);
                    }
                    return flussValue;
                case TIME:
                    if (flussValue instanceof Long) {
                        return LocalTime.ofNanoOfDay(
                                (Long) flussValue * 1000); // Convert from microseconds
                    }
                    return flussValue;
                case TIMESTAMP:
                    if (flussValue instanceof TimestampNtz) {
                        return ((TimestampNtz) flussValue).toLocalDateTime();
                    }
                    return flussValue;
                case DECIMAL:
                    if (flussValue instanceof Decimal) {
                        return ((Decimal) flussValue).toBigDecimal();
                    }
                    if (flussValue instanceof BigDecimal) {
                        return flussValue;
                    }
                    return new BigDecimal(flussValue.toString());
                case ARRAY:
                    return convertArrayFromFluss((ArrayType<?, ?>) seaTunnelType, flussValue);
                case MAP:
                    return convertMapFromFluss((MapType<?, ?>) seaTunnelType, flussValue);
                case ROW:
                    return convertRowFromFluss((SeaTunnelRowType) seaTunnelType, flussValue);
                default:
                    throw new FlussConnectorException(
                            FlussConnectorErrorCode.UNSUPPORTED_OPERATION,
                            "Unsupported SeaTunnel data type: " + seaTunnelType.getSqlType());
            }
        } catch (Exception e) {
            throw new FlussConnectorException(
                    FlussConnectorErrorCode.SERIALIZATION_FAILED,
                    "Failed to convert Fluss type to SeaTunnel type",
                    e);
        }
    }

    private static Object convertArrayToFluss(ArrayType<?, ?> arrayType, Object value) {
        // Convert array elements
        if (value instanceof Object[]) {
            Object[] array = (Object[]) value;
            Object[] converted = new Object[array.length];
            for (int i = 0; i < array.length; i++) {
                converted[i] = convertToFlussType(arrayType.getElementType(), array[i]);
            }
            return converted;
        }
        return value;
    }

    private static Object convertArrayFromFluss(ArrayType<?, ?> arrayType, Object flussValue) {
        // Convert array elements from Fluss
        if (flussValue instanceof Object[]) {
            Object[] array = (Object[]) flussValue;
            Object[] converted = new Object[array.length];
            for (int i = 0; i < array.length; i++) {
                converted[i] = convertFromFlussType(arrayType.getElementType(), array[i]);
            }
            return converted;
        }
        return flussValue;
    }

    private static Object convertMapToFluss(MapType<?, ?> mapType, Object value) {
        // Convert map key-value pairs
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<Object, Object> converted = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object convertedKey = convertToFlussType(mapType.getKeyType(), entry.getKey());
                Object convertedValue =
                        convertToFlussType(mapType.getValueType(), entry.getValue());
                converted.put(convertedKey, convertedValue);
            }
            return converted;
        }
        return value;
    }

    private static Object convertMapFromFluss(MapType<?, ?> mapType, Object flussValue) {
        // Convert map key-value pairs from Fluss
        if (flussValue instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) flussValue;
            Map<Object, Object> converted = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object convertedKey = convertFromFlussType(mapType.getKeyType(), entry.getKey());
                Object convertedValue =
                        convertFromFlussType(mapType.getValueType(), entry.getValue());
                converted.put(convertedKey, convertedValue);
            }
            return converted;
        }
        return flussValue;
    }

    private static Object convertRowToFluss(SeaTunnelRowType rowType, SeaTunnelRow row) {
        // Convert SeaTunnelRow to Fluss row format
        Object[] fields = new Object[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
            fields[i] = convertToFlussType(rowType.getFieldType(i), row.getField(i));
        }
        return fields;
    }

    private static SeaTunnelRow convertRowFromFluss(SeaTunnelRowType rowType, Object flussValue) {
        // Convert Fluss row to SeaTunnelRow
        if (flussValue instanceof Object[]) {
            Object[] fields = (Object[]) flussValue;
            Object[] converted = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                converted[i] = convertFromFlussType(rowType.getFieldType(i), fields[i]);
            }
            return new SeaTunnelRow(converted);
        }
        throw new FlussConnectorException(
                FlussConnectorErrorCode.SERIALIZATION_FAILED, "Invalid Fluss row format");
    }

    /** Get field value from Fluss InternalRow based on field type */
    private static Object getFieldValue(
            InternalRow row, int index, SeaTunnelDataType<?> fieldType) {
        switch (fieldType.getSqlType()) {
            case BOOLEAN:
                return row.getBoolean(index);
            case TINYINT:
                return row.getByte(index);
            case SMALLINT:
                return row.getShort(index);
            case INT:
                return row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            case FLOAT:
                return row.getFloat(index);
            case DOUBLE:
                return row.getDouble(index);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
            case STRING:
                return row.getString(index);
            case BYTES:
                return row.getBinary(index);
            case DATE:
                return row.getInt(index);
            case TIME:
                return row.getLong(index);
            case TIMESTAMP:
                return row.getTimestampLtz(index, 6); // Assuming microsecond precision
            default:
                return row.getField(index);
        }
    }
}
