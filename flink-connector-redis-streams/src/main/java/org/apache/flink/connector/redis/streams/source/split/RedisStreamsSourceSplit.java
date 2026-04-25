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
 * Immutable Redis Stream split: identity is {@link #streamKey}; {@link #startingEntryId} is an
 * informational resume position (canonical state lives in the Redis consumer group);
 * {@link #stoppingEntryId} is non-null only in bounded mode.
 */
@Internal
public class RedisStreamsSourceSplit implements SourceSplit {

    private final String streamKey;
    @Nullable private final String startingEntryId;
    @Nullable private final String stoppingEntryId;

    public RedisStreamsSourceSplit(
            String streamKey, @Nullable String startingEntryId, @Nullable String stoppingEntryId) {
        this.streamKey = Preconditions.checkNotNull(streamKey, "streamKey");
        this.startingEntryId = startingEntryId;
        this.stoppingEntryId = stoppingEntryId;
    }

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
