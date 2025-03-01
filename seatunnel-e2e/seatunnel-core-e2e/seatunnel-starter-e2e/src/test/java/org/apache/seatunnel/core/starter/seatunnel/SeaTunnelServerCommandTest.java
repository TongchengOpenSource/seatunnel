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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class SeaTunnelServerCommandTest extends TestSuiteBase implements TestResource {

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testExecCheck(TestContainer container) throws Exception {

        //        String[] args = {"-cn", "test","-d", "-r", "master_and_worker", "-m"};
        String[] case1 = {"-m, -cn cluster"};
        Container.ExecResult execResult = execCommand(container, case1);
    }

    private Container.ExecResult execCommand(TestContainer container, String[] case1)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeServerCommand(case1);
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertTrue(StringUtils.isBlank(execResult.getStderr()));
        log.info(execResult.getStdout());
        return execResult;
    }
}
