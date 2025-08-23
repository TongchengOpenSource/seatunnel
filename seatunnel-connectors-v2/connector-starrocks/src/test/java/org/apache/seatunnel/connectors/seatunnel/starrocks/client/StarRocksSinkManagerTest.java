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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class StarRocksSinkManagerTest {

    private SinkConfig mockSinkConfig;
    private TableSchema mockTableSchema;
    private AbstractStreamLoadVisitor mockStreamLoadVisitor;
    private StarRocksSinkManager sinkManager;

    @BeforeEach
    void setUp() {
        mockSinkConfig = mock(SinkConfig.class);
        mockTableSchema = mock(TableSchema.class);
        mockStreamLoadVisitor = mock(AbstractStreamLoadVisitor.class);

        when(mockSinkConfig.getBatchMaxSize()).thenReturn(10);
        when(mockSinkConfig.getBatchMaxBytes()).thenReturn(1024 * 1024 * 1024L);
        when(mockSinkConfig.getNodeUrls()).thenReturn(Arrays.asList("localhost:8030"));
        when(mockSinkConfig.isEnable2PC()).thenReturn(false);

        this.sinkManager = new StarRocksSinkManager(mockSinkConfig, mockTableSchema, mockStreamLoadVisitor);
    }

    @Test
    void testNormalModeInitialization() {
        when(mockSinkConfig.isEnable2PC()).thenReturn(false);

        StarRocksSinkManager manager = new StarRocksSinkManager(mockSinkConfig, mockTableSchema);

        assertNotNull(manager);
    }

    @Test
    void testTransactionModeInitialization() {
        when(mockSinkConfig.isEnable2PC()).thenReturn(true);

        StarRocksSinkManager manager = new StarRocksSinkManager(mockSinkConfig, mockTableSchema);

        assertNotNull(manager);
    }

    @Test
    void testWriteAndFlushInNormalMode() throws IOException {
        when(mockSinkConfig.getBatchMaxSize()).thenReturn(1); // Force flush after 1 record
        when(mockStreamLoadVisitor.doStreamLoad(any(StarRocksFlushTuple.class))).thenReturn(true);

        sinkManager.write("test-record");

        // Verify that doStreamLoad was called on the visitor
        verify(mockStreamLoadVisitor, times(1)).doStreamLoad(any(StarRocksFlushTuple.class));
    }

    @Test
    void testPrepareCommitInNormalMode() throws IOException {
        when(mockSinkConfig.isEnable2PC()).thenReturn(false);

        StarRocksCommitInfo commitInfo = sinkManager.prepareCommit();

        assertNull(commitInfo); // Normal mode should return null
    }

    @Test
    void testAbortTransactionInNormalMode() {
        when(mockSinkConfig.isEnable2PC()).thenReturn(false);

        // Should not throw exception in normal mode
        assertDoesNotThrow(() -> sinkManager.abortTransaction());
    }

    @Test
    void testClose() throws IOException {
        sinkManager.close();

        // Verify that close was called on the visitor
        verify(mockStreamLoadVisitor, times(1)).close();
    }

    @Test
    void testPrepareCommitDelegation() throws IOException {
        StarRocksCommitInfo expectedCommitInfo = new StarRocksCommitInfo("host", "label", "db");
        when(mockStreamLoadVisitor.prepareCommit()).thenReturn(expectedCommitInfo);

        StarRocksCommitInfo actualCommitInfo = sinkManager.prepareCommit();

        assertEquals(expectedCommitInfo, actualCommitInfo);
        verify(mockStreamLoadVisitor, times(1)).prepareCommit();
    }

    @Test
    void testAbortTransactionDelegation() {
        sinkManager.abortTransaction();

        verify(mockStreamLoadVisitor, times(1)).abortTransaction();
    }
}
