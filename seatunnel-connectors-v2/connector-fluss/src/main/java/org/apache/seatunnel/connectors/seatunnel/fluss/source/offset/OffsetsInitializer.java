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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * An interface for users to specify the starting offset of Fluss source splits.
 * 
 * <p>This interface provides a flexible way to initialize offsets for different scenarios:
 * <ul>
 *   <li>Reading from earliest available offsets</li>
 *   <li>Reading from latest offsets</li>
 *   <li>Reading from a specific timestamp</li>
 *   <li>Performing a full snapshot then reading changelog</li>
 * </ul>
 * 
 * <p>Example usage:
 * <pre>{@code
 * // Start from earliest
 * OffsetsInitializer earliest = OffsetsInitializer.earliest();
 * 
 * // Start from latest
 * OffsetsInitializer latest = OffsetsInitializer.latest();
 * 
 * // Start from timestamp
 * OffsetsInitializer timestamp = OffsetsInitializer.timestamp(1678883047356L);
 * 
 * // Full snapshot then changelog
 * OffsetsInitializer full = OffsetsInitializer.full();
 * }</pre>
 */
public interface OffsetsInitializer extends Serializable {

    /**
     * Get the initial offsets for the given Fluss buckets. These offsets will be used as starting
     * offsets of the Fluss buckets.
     *
     * @param partitionName the partition name of the buckets if they are partitioned. Otherwise, null.
     * @param buckets the Fluss buckets to get the starting offsets.
     * @param bucketOffsetsRetriever a helper to retrieve information of the Fluss buckets.
     * @return A mapping from Fluss bucket to their offsets to start scanning from.
     */
    Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever);

    /**
     * Get the startup mode associated with this offset initializer.
     *
     * @return the startup mode
     */
    StartupMode getStartupMode();

    /**
     * Get the timestamp for timestamp-based initialization.
     *
     * @return the timestamp in milliseconds, or null if not applicable
     */
    @Nullable
    Long getTimestamp();

    /**
     * An interface that provides necessary information to the {@link OffsetsInitializer} to get the
     * initial offsets of the Fluss buckets.
     */
    interface BucketOffsetsRetriever {
        
        /**
         * Get the latest offsets for the specified buckets.
         *
         * @param partitionName the partition name, or null for non-partitioned tables
         * @param buckets the buckets to get offsets for
         * @return a map of bucket to latest offset
         */
        Map<Integer, Long> latestOffsets(
                @Nullable String partitionName, Collection<Integer> buckets);

        /**
         * Get the earliest offsets for the specified buckets.
         *
         * @param partitionName the partition name, or null for non-partitioned tables
         * @param buckets the buckets to get offsets for
         * @return a map of bucket to earliest offset
         */
        Map<Integer, Long> earliestOffsets(
                @Nullable String partitionName, Collection<Integer> buckets);

        /**
         * Get the offsets for the specified buckets based on timestamp.
         *
         * @param partitionName the partition name, or null for non-partitioned tables
         * @param buckets the buckets to get offsets for
         * @param timestamp the timestamp in milliseconds
         * @return a map of bucket to offset at the specified timestamp
         */
        Map<Integer, Long> offsetsFromTimestamp(
                @Nullable String partitionName, Collection<Integer> buckets, long timestamp);

        /**
         * Get the snapshot offsets for the specified buckets.
         *
         * @param partitionName the partition name, or null for non-partitioned tables
         * @param buckets the buckets to get offsets for
         * @return a map of bucket to snapshot offset
         */
        Map<Integer, Long> snapshotOffsets(
                @Nullable String partitionName, Collection<Integer> buckets);
    }

    // --------------- Factory methods ---------------

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     * offsets of each bucket.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the earliest available offsets.
     */
    static OffsetsInitializer earliest() {
        return new EarliestOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the latest offsets of each bucket.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the latest offsets.
     */
    static OffsetsInitializer latest() {
        return new LatestOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which performs a full snapshot on the table upon first
     * startup, and continue to read log with the offset to the snapshot.
     *
     * <p>If the table to read is a log table, the full snapshot means reading from the earliest log
     * offset which means "full" OffsetsInitializer equal to the {@link #earliest()}
     * OffsetsInitializer. If the table to read is a primary key table, the full snapshot means
     * reading the latest snapshot which materializes all changes on the table.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to snapshot offsets.
     */
    static OffsetsInitializer full() {
        return new FullOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets in each bucket so that the
     * initialized offset is the offset of the first record batch whose commit timestamp is greater
     * than or equals the given timestamp (milliseconds).
     *
     * @param timestamp the timestamp (milliseconds) to start the scan.
     * @return an {@link OffsetsInitializer} which initializes the offsets based on the given timestamp.
     */
    static OffsetsInitializer timestamp(long timestamp) {
        return new TimestampOffsetsInitializer(timestamp);
    }

    /**
     * Create an OffsetsInitializer from StartupMode and optional timestamp.
     *
     * @param startupMode the startup mode
     * @param timestamp the timestamp for timestamp mode, can be null for other modes
     * @return the appropriate OffsetsInitializer
     * @throws IllegalArgumentException if timestamp is required but not provided, or if timestamp is invalid
     */
    static OffsetsInitializer fromStartupMode(StartupMode startupMode, @Nullable Long timestamp) {
        switch (startupMode) {
            case EARLIEST:
                return earliest();
            case LATEST:
                return latest();
            case FULL:
                return full();
            case TIMESTAMP:
                if (timestamp == null) {
                    throw new IllegalArgumentException(
                            "Timestamp must be provided when using TIMESTAMP startup mode");
                }
                if (timestamp <= 0) {
                    throw new IllegalArgumentException(
                            "Timestamp must be positive, got: " + timestamp);
                }
                return timestamp(timestamp);
            default:
                throw new IllegalArgumentException("Unsupported startup mode: " + startupMode);
        }
    }
}
