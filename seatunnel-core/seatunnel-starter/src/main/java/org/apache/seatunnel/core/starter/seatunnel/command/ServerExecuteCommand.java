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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.exception.ErrorLimitConfig;
import org.apache.seatunnel.core.starter.exception.ErrorRecordTracker;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;

/** This command is used to execute the SeaTunnel engine job by SeaTunnel API. */
@Slf4j
public class ServerExecuteCommand implements Command<ServerCommandArgs> {

    private final ServerCommandArgs serverCommandArgs;

    public ServerExecuteCommand(ServerCommandArgs serverCommandArgs) {
        this.serverCommandArgs = serverCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        checkEnvironment();
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        
        // 从环境变量读取错误限制配置
        Map<String, Object> envConfig = new HashMap<>();
        
        // 读取错误记录数限制
        String errorLimitCount = System.getenv("ERROR_LIMIT_COUNT");
        if (errorLimitCount != null) {
            envConfig.put("error.limit.record", Integer.parseInt(errorLimitCount));
        }
        
        // 读取错误比例限制 
        String errorLimitPercentage = System.getenv("ERROR_LIMIT_PERCENTAGE");
        if (errorLimitPercentage != null) {
            envConfig.put("error.limit.percentage", Double.parseDouble(errorLimitPercentage));
        }

        // 合并环境变量配置到 SeaTunnelConfig
        if (!envConfig.isEmpty()) {
            Config envConfigObj = ConfigFactory.parseMap(envConfig);
            seaTunnelConfig = seaTunnelConfig.withFallback(envConfigObj);
        }

        String clusterRole = this.serverCommandArgs.getClusterRole();
        if (StringUtils.isNotBlank(clusterRole)) {
            if (EngineConfig.ClusterRole.MASTER.toString().equalsIgnoreCase(clusterRole)) {
                seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER);
            } else if (EngineConfig.ClusterRole.WORKER.toString().equalsIgnoreCase(clusterRole)) {
                seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.WORKER);

                // in hazelcast lite node will not store IMap data.
                seaTunnelConfig.getHazelcastConfig().setLiteMember(true);
            } else {
                throw new SeaTunnelEngineException("Not supported cluster role: " + clusterRole);
            }
        } else {
            seaTunnelConfig
                    .getEngineConfig()
                    .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        }

        SeaTunnelServerStarter.createHazelcastInstance(
                seaTunnelConfig, Thread.currentThread().getName());

        RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
        ErrorLimitConfig errorLimitConfig = runtimeEnvironment.getErrorLimitConfig(seaTunnelConfig);
        
        // 创建错误跟踪器
        ErrorRecordTracker errorTracker = new ErrorRecordTracker(errorLimitConfig);
        
        try {
            // 在处理数据时使用错误跟踪器
            while (hasNextRecord) {
                try {
                    errorTracker.recordTotal();
                    processRecord(record);
                } catch (Exception e) {
                    errorTracker.recordError();
                    log.error("Error processing record: " + record, e);
                }
            }
        } catch (Exception e) {
            if (e.getMessage().contains("Error record count exceeded") 
                || e.getMessage().contains("Error percentage exceeded")) {
                throw new TaskExecuteException("Job failed due to error limit exceeded: " + e.getMessage());
            }
            throw e;
        }
    }

    private void checkEnvironment() {
        if (isAllocatingThreadGetName()) {
            log.warn(
                    "The current JDK version is not recommended. Please upgrade to JDK 1.8.0_102 or higher. "
                            + "The current version will affect the performance of log printing. "
                            + "For details, please refer to https://issues.apache.org/jira/browse/LOG4J2-2052");
        }
    }

    static boolean isAllocatingThreadGetName() {
        // LOG4J2-2052, LOG4J2-2635 JDK 8u102 ("1.8.0_102") removed the String allocation in
        // Thread.getName()
        if (SystemUtils.IS_JAVA_1_8) {
            try {
                Pattern javaVersionPattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)_(\\d+)");
                Matcher m = javaVersionPattern.matcher(System.getProperty("java.version"));
                if (m.matches()) {
                    return Integer.parseInt(m.group(3)) == 0 && Integer.parseInt(m.group(4)) < 102;
                }
                return true;
            } catch (Exception e) {
                return true;
            }
        } else {
            return !SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8);
        }
    }
}
