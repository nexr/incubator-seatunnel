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

package org.apache.seatunnel.transform;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import java.util.UUID;

@AutoService(SeaTunnelTransform.class)
public class UUIDTransform extends SingleFieldOutputTransform {

    public static final Option<String> UUID_FIELD =
            Options.key("uuid_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("UUID field");

    private String uuidField;

    @Override
    public String getPluginName() {
        return "UUID";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(pluginConfig, UUID_FIELD.key());
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }

        this.uuidField = pluginConfig.getString(UUID_FIELD.key());
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType rowType) {
    }

    @Override
    protected String getOutputFieldName() {
        return uuidField;
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        return getUUID();
    }

    private Object getUUID() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }
}
