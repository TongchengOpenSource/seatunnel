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
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} that performs a full snapshot on the table upon
 * first startup, and continues to read the changelog.
 *
 * <p>The behavior depends on the table type:
 *
 * <ul>
 *   <li>For log tables: equivalent to {@link EarliestOffsetsInitializer}, reading from the earliest
 *       offset
 *   <li>For primary key tables: reads the latest snapshot which materializes all changes on the
 *       table
 * </ul>
 *
 * <p>This mode is useful when you want to get a complete view of the data, including both
 * historical data and ongoing changes.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer#full()}.
 */
class FullOffsetsInitializer implements OffsetsInitializer {

    private static final long serialVersionUID = 1L;

    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {

        // For full mode, we try to get snapshot offsets first.
        // If snapshot offsets are not available (e.g., for log tables),
        // fall back to earliest offsets.
        try {
            return bucketOffsetsRetriever.snapshotOffsets(partitionName, buckets);
        } catch (Exception e) {
            // Fall back to earliest offsets for log tables or when snapshot is not available
            return bucketOffsetsRetriever.earliestOffsets(partitionName, buckets);
        }
    }

    @Override
    public StartupMode getStartupMode() {
        return StartupMode.FULL;
    }

    @Override
    public Long getTimestamp() {
        return null;
    }

    @Override
    public String toString() {
        return "FullOffsetsInitializer{}";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FullOffsetsInitializer;
    }

    @Override
    public int hashCode() {
        return FullOffsetsInitializer.class.hashCode();
    }
}
