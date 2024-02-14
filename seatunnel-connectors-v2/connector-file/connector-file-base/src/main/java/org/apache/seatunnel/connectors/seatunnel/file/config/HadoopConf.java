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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import lombok.Data;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Data
public class HadoopConf implements Serializable {
    private static final String HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";
    private static final String SCHEMA = "hdfs";
    protected Map<String, String> extraOptions = new HashMap<>();
    protected String hdfsNameKey;
    protected String hdfsSitePath;
    protected String hdfsSiteXmlProperties;
    protected String kerberosPrincipal;
    protected String kerberosKeytabPath;

    public HadoopConf(String hdfsNameKey) {
        this.hdfsNameKey = hdfsNameKey;
    }

    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    public String getSchema() {
        return SCHEMA;
    }

    public void setExtraOptionsForConfiguration(Configuration configuration) {
        if (!extraOptions.isEmpty()) {
            extraOptions.forEach(configuration::set);
        }
        if (hdfsSitePath != null) {
            configuration.addResource(new Path(hdfsSitePath));
        }
        if (hdfsSiteXmlProperties != null) {
            try (ByteArrayInputStream inputStream =
                         new ByteArrayInputStream(hdfsSiteXmlProperties.getBytes(StandardCharsets.UTF_8))) {
                configuration.addResource(inputStream);
            } catch (IOException e) {
                throw new FileConnectorException(FileConnectorErrorCode.DATA_DESERIALIZE_FAILED, e.getMessage());
            }
        }
    }
}
