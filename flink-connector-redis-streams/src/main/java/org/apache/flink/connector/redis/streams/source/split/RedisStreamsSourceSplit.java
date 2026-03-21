/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.redis.streams.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Immutable split representing a Redis Stream key.
 *
 * <p>A split is identified solely by its {@link #streamKey} — at most one split exists per stream
 * key, and at most one subtask reads any given stream. Setting job parallelism higher than the
 * number of configured stream keys results in idle subtasks. This is intentional: Redis Streams
 * guarantee total ordering within a stream, and splitting a single stream across multiple consumers
 * via a consumer group breaks that guarantee.
 *
 * <p>The split's {@link #startingEntryId} is the resume position written into the snapshot at
 * checkpoint time. It is informational — the canonical position is held server-side by the Redis
 * consumer group. {@link #stoppingEntryId} is set by the enumerator in bounded mode to the stream's
 * last-generated-id at job startup; the reader stops a split once it has consumed past this ID. In
 * unbounded mode, {@code stoppingEntryId} is {@code null}.
 *
 * <p>Mutable per-split runtime state lives on {@link RedisStreamsSourceSplitState}.
 */
@Internal
public class RedisStreamsSourceSplit implements SourceSplit {

    /** The Redis stream key this split reads from (split identity). */
    private final String streamKey;

    /**
     * The entry ID from which to resume reading on restore. {@code null} on a fresh split — the
     * first read uses the consumer group's last-consumed offset (server-side) or the startup-mode
     * offset if the group does not yet exist.
     */
    @Nullable private final String startingEntryId;

    /**
     * Inclusive upper bound for bounded mode. The split is considered finished once an entry with
     * ID {@code > stoppingEntryId} would be returned. {@code null} for unbounded streaming.
     */
    @Nullable private final String stoppingEntryId;

    public RedisStreamsSourceSplit(
            String streamKey, @Nullable String startingEntryId, @Nullable String stoppingEntryId) {
        this.streamKey = Preconditions.checkNotNull(streamKey, "streamKey");
        this.startingEntryId = startingEntryId;
        this.stoppingEntryId = stoppingEntryId;
    }

    /** Convenience constructor for unbounded streaming with no resume position. */
    public RedisStreamsSourceSplit(String streamKey) {
        this(streamKey, null, null);
    }

    @Override
    public String splitId() {
        return streamKey;
    }

    public String getStreamKey() {
        return streamKey;
    }

    @Nullable
    public String getStartingEntryId() {
        return startingEntryId;
    }

    @Nullable
    public String getStoppingEntryId() {
        return stoppingEntryId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisStreamsSourceSplit that = (RedisStreamsSourceSplit) o;
        return streamKey.equals(that.streamKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamKey);
    }

    @Override
    public String toString() {
        return "RedisStreamsSourceSplit{"
                + "streamKey='"
                + streamKey
                + '\''
                + ", startingEntryId='"
                + startingEntryId
                + '\''
                + ", stoppingEntryId='"
                + stoppingEntryId
                + '\''
                + '}';
    }
}
