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

package org.apache.seatunnel.transform.uuid;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class UUIDTransform extends SingleFieldOutputTransform {
    public static final String PLUGIN_NAME = "UUID";

    private UUIDTransformConfig uuidTransformConfig;
    private String uuidField;

    public UUIDTransform(
            @NonNull UUIDTransformConfig uuidTransformConfig,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.uuidTransformConfig = uuidTransformConfig;
        uuidField = uuidTransformConfig.getUuidField();
        if (uuidField.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find [" + uuidField + "] field");
        }
        this.outputCatalogTable = getProducedCatalogTable();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
                .validate(new UUIDTransformFactory().optionRule());
        this.uuidTransformConfig = UUIDTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType rowType) {}

    @Override
    protected String getOutputFieldName() {
        return uuidTransformConfig.getUuidField();
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        return getUUID();
    }

    @Override
    protected Column getOutputColumn() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        List<Column> collect =
                columns.stream()
                        .filter(
                                column ->
                                        column.getName()
                                                .equals(
                                                        uuidTransformConfig.getUuidField()))
                        .collect(Collectors.toList());
        return collect.get(0).copy();
    }

    private Object getUUID() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }
}
