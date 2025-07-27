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

package org.apache.seatunnel.connectors.seatunnel.fluss.source.offset;

import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartupMode;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link OffsetsInitializer} and its implementations.
 */
class OffsetsInitializerTest {

    private static final Collection<Integer> TEST_BUCKETS = Arrays.asList(0, 1, 2);
    private static final String TEST_PARTITION = "test_partition";

    @Test
    void testEarliestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.earliest();
        
        assertEquals(StartupMode.EARLIEST, initializer.getStartupMode());
        assertNull(initializer.getTimestamp());
        
        MockBucketOffsetsRetriever retriever = new MockBucketOffsetsRetriever();
        Map<Integer, Long> offsets = initializer.getBucketOffsets(TEST_PARTITION, TEST_BUCKETS, retriever);
        
        assertEquals(3, offsets.size());
        for (Integer bucket : TEST_BUCKETS) {
            assertEquals(EarliestOffsetsInitializer.EARLIEST_OFFSET, offsets.get(bucket));
        }
    }

    @Test
    void testLatestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.latest();
        
        assertEquals(StartupMode.LATEST, initializer.getStartupMode());
        assertNull(initializer.getTimestamp());
        
        MockBucketOffsetsRetriever retriever = new MockBucketOffsetsRetriever();
        Map<Integer, Long> offsets = initializer.getBucketOffsets(TEST_PARTITION, TEST_BUCKETS, retriever);
        
        assertEquals(3, offsets.size());
        for (Integer bucket : TEST_BUCKETS) {
            assertEquals(MockBucketOffsetsRetriever.LATEST_OFFSET, offsets.get(bucket));
        }
    }

    @Test
    void testFullOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.full();
        
        assertEquals(StartupMode.FULL, initializer.getStartupMode());
        assertNull(initializer.getTimestamp());
        
        MockBucketOffsetsRetriever retriever = new MockBucketOffsetsRetriever();
        Map<Integer, Long> offsets = initializer.getBucketOffsets(TEST_PARTITION, TEST_BUCKETS, retriever);
        
        assertEquals(3, offsets.size());
        for (Integer bucket : TEST_BUCKETS) {
            assertEquals(MockBucketOffsetsRetriever.SNAPSHOT_OFFSET, offsets.get(bucket));
        }
    }

    @Test
    void testTimestampOffsetsInitializer() {
        long timestamp = 1678883047356L;
        OffsetsInitializer initializer = OffsetsInitializer.timestamp(timestamp);
        
        assertEquals(StartupMode.TIMESTAMP, initializer.getStartupMode());
        assertEquals(timestamp, initializer.getTimestamp());
        
        MockBucketOffsetsRetriever retriever = new MockBucketOffsetsRetriever();
        Map<Integer, Long> offsets = initializer.getBucketOffsets(TEST_PARTITION, TEST_BUCKETS, retriever);
        
        assertEquals(3, offsets.size());
        for (Integer bucket : TEST_BUCKETS) {
            assertEquals(timestamp / 1000, offsets.get(bucket));
        }
    }

    @Test
    void testTimestampOffsetsInitializerInvalidTimestamp() {
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializer.timestamp(0));
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializer.timestamp(-1));
    }

    @Test
    void testFromStartupMode() {
        // Test EARLIEST
        OffsetsInitializer earliest = OffsetsInitializer.fromStartupMode(StartupMode.EARLIEST, null);
        assertEquals(StartupMode.EARLIEST, earliest.getStartupMode());
        
        // Test LATEST
        OffsetsInitializer latest = OffsetsInitializer.fromStartupMode(StartupMode.LATEST, null);
        assertEquals(StartupMode.LATEST, latest.getStartupMode());
        
        // Test FULL
        OffsetsInitializer full = OffsetsInitializer.fromStartupMode(StartupMode.FULL, null);
        assertEquals(StartupMode.FULL, full.getStartupMode());
        
        // Test TIMESTAMP with valid timestamp
        long timestamp = 1678883047356L;
        OffsetsInitializer timestampInit = OffsetsInitializer.fromStartupMode(StartupMode.TIMESTAMP, timestamp);
        assertEquals(StartupMode.TIMESTAMP, timestampInit.getStartupMode());
        assertEquals(timestamp, timestampInit.getTimestamp());
    }

    @Test
    void testFromStartupModeInvalidCombinations() {
        // TIMESTAMP mode without timestamp
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializer.fromStartupMode(StartupMode.TIMESTAMP, null));
        
        // TIMESTAMP mode with invalid timestamp
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializer.fromStartupMode(StartupMode.TIMESTAMP, 0L));
        
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializer.fromStartupMode(StartupMode.TIMESTAMP, -1L));
    }

    @Test
    void testEqualsAndHashCode() {
        // Test EarliestOffsetsInitializer
        OffsetsInitializer earliest1 = OffsetsInitializer.earliest();
        OffsetsInitializer earliest2 = OffsetsInitializer.earliest();
        assertEquals(earliest1, earliest2);
        assertEquals(earliest1.hashCode(), earliest2.hashCode());
        
        // Test TimestampOffsetsInitializer
        long timestamp = 1678883047356L;
        OffsetsInitializer timestamp1 = OffsetsInitializer.timestamp(timestamp);
        OffsetsInitializer timestamp2 = OffsetsInitializer.timestamp(timestamp);
        assertEquals(timestamp1, timestamp2);
        assertEquals(timestamp1.hashCode(), timestamp2.hashCode());
        
        // Test different timestamps
        OffsetsInitializer timestamp3 = OffsetsInitializer.timestamp(timestamp + 1000);
        assertNotEquals(timestamp1, timestamp3);
    }

    /**
     * Mock implementation of BucketOffsetsRetriever for testing.
     */
    private static class MockBucketOffsetsRetriever implements OffsetsInitializer.BucketOffsetsRetriever {
        
        static final long LATEST_OFFSET = 1000L;
        static final long EARLIEST_OFFSET = 0L;
        static final long SNAPSHOT_OFFSET = 500L;

        @Override
        public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, LATEST_OFFSET);
            }
            return offsets;
        }

        @Override
        public Map<Integer, Long> earliestOffsets(String partitionName, Collection<Integer> buckets) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, EARLIEST_OFFSET);
            }
            return offsets;
        }

        @Override
        public Map<Integer, Long> offsetsFromTimestamp(String partitionName, Collection<Integer> buckets, long timestamp) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, timestamp / 1000); // Simple calculation for testing
            }
            return offsets;
        }

        @Override
        public Map<Integer, Long> snapshotOffsets(String partitionName, Collection<Integer> buckets) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, SNAPSHOT_OFFSET);
            }
            return offsets;
        }
    }
}
