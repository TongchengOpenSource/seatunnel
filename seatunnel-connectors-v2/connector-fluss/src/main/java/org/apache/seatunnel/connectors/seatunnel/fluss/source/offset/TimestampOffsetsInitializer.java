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
import java.util.Objects;

/**
 * An implementation of {@link OffsetsInitializer} that initializes the offsets based on a
 * timestamp.
 *
 * <p>This initializer will start reading from the first record batch whose commit timestamp is
 * greater than or equal to the specified timestamp. This is useful for scenarios where you want to
 * replay data from a specific point in time.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer#timestamp(long)}.
 */
class TimestampOffsetsInitializer implements OffsetsInitializer {

    private static final long serialVersionUID = 1L;

    private final long timestamp;

    /**
     * Creates a new TimestampOffsetsInitializer.
     *
     * @param timestamp the timestamp in milliseconds since epoch
     * @throws IllegalArgumentException if timestamp is not positive
     */
    TimestampOffsetsInitializer(long timestamp) {
        if (timestamp <= 0) {
            throw new IllegalArgumentException("Timestamp must be positive, got: " + timestamp);
        }
        this.timestamp = timestamp;
    }

    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {

        return bucketOffsetsRetriever.offsetsFromTimestamp(partitionName, buckets, timestamp);
    }

    @Override
    public StartupMode getStartupMode() {
        return StartupMode.TIMESTAMP;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "TimestampOffsetsInitializer{timestamp=" + timestamp + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimestampOffsetsInitializer that = (TimestampOffsetsInitializer) obj;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp);
    }
}
