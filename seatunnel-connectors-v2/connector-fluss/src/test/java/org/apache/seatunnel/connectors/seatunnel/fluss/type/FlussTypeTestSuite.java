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

import org.apache.seatunnel.connectors.seatunnel.fluss.data.FlussTypeConvertTest;
import org.apache.seatunnel.connectors.seatunnel.fluss.utils.FlussRowTypeConverterTest;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for all Fluss type conversion tests.
 *
 * <p>This suite includes:
 *
 * <ul>
 *   <li>FlussTypeConverterTest - Tests for the main type converter
 *   <li>FlussRowTypeConverterTest - Tests for the row type converter
 *   <li>FlussTypeMapperTest - Tests for the type mapper
 *   <li>FlussTypeIntegrationTest - Integration tests for the complete flow
 * </ul>
 */
@Suite
@SelectClasses({
    FlussTypeConverterTest.class,
    FlussRowTypeConverterTest.class,
    FlussTypeConvertTest.class,
    FlussTypeIntegrationTest.class
})
public class FlussTypeTestSuite {
    // Test suite class - no implementation needed
}
