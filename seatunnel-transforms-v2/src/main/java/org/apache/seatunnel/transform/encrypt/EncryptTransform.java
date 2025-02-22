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

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;

public class EncryptTransform extends SingleFieldOutputTransform {

    public static String PLUGIN_NAME = "Encrypt";

    private final ReadonlyConfig config;
    private int inputFieldIndex;
    private Encrypts encrypts;

    public EncryptTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.encrypts =
                Encrypts.valueOf(
                        StringUtils.upperCase(
                                this.config.get(EncryptTransformConfig.ENCRYPT_NAME)));

        initOutputFields(
                inputCatalogTable.getTableSchema().toPhysicalRowDataType(),
                this.config.get(EncryptTransformConfig.KEY_ENCRYPT_FIELD));
    }

    private void initOutputFields(SeaTunnelRowType inputRowType, String encryptField) {
        try {
            inputFieldIndex = inputRowType.indexOf(encryptField);
        } catch (IllegalArgumentException e) {
            throw TransformCommonError.cannotFindInputFieldError(getPluginName(), encryptField);
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object fieldValue = getOutputFieldValue(new SeaTunnelRowAccessor(inputRow));
        inputRow.setRowKind(RowKind.INSERT);
        SeaTunnelRow outputRow = getRowContainerGenerator().apply(inputRow);
        outputRow.setField(getFieldIndex(), fieldValue);
        return outputRow;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        Object inputFieldValue = inputRow.getField(inputFieldIndex);
        if (inputFieldValue == null) {
            return null;
        }
        return encrypts.encrypt(inputFieldValue.toString());
    }

    @Override
    protected Column getOutputColumn() {
        return null;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
}
