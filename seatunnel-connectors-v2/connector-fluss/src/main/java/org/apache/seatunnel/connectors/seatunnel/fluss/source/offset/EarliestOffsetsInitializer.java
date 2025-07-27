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

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} that initializes the buckets to the earliest offsets.
 * 
 * <p>This initializer will start reading from the earliest available offset in each bucket,
 * which means reading all available data from the beginning of the log.
 * 
 * <p>Package private and should be instantiated via {@link OffsetsInitializer#earliest()}.
 */
class EarliestOffsetsInitializer implements OffsetsInitializer {

    private static final long serialVersionUID = 1L;

    /**
     * Special offset value indicating the earliest available offset.
     * This follows the same convention as Kafka's EARLIEST_OFFSET.
     */
    public static final long EARLIEST_OFFSET = -2L;

    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {
        
        Map<Integer, Long> initialOffsets = new HashMap<>();
        for (Integer bucket : buckets) {
            initialOffsets.put(bucket, EARLIEST_OFFSET);
        }
        return initialOffsets;
    }

    @Override
    public StartupMode getStartupMode() {
        return StartupMode.EARLIEST;
    }

    @Override
    public Long getTimestamp() {
        return null;
    }

    @Override
    public String toString() {
        return "EarliestOffsetsInitializer{}";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof EarliestOffsetsInitializer;
    }

    @Override
    public int hashCode() {
        return EarliestOffsetsInitializer.class.hashCode();
    }
}
