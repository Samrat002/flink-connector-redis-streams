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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

/** Mutable per-split state — the latest entry ID emitted by the record emitter. */
@Internal
public class RedisStreamsSourceSplitState {

    private final RedisStreamsSourceSplit split;
    @Nullable private String currentEntryId;

    public RedisStreamsSourceSplitState(RedisStreamsSourceSplit split) {
        this.split = Preconditions.checkNotNull(split, "split");
        this.currentEntryId = split.getStartingEntryId();
    }

    public String getStreamKey() {
        return split.getStreamKey();
    }

    @Nullable
    public String getCurrentEntryId() {
        return currentEntryId;
    }

    public void setCurrentEntryId(String currentEntryId) {
        this.currentEntryId = currentEntryId;
    }

    public RedisStreamsSourceSplit toSplit() {
        return new RedisStreamsSourceSplit(
                split.getStreamKey(), currentEntryId, split.getStoppingEntryId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisStreamsSourceSplitState that = (RedisStreamsSourceSplitState) o;
        return split.equals(that.split) && Objects.equals(currentEntryId, that.currentEntryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(split, currentEntryId);
    }

    @Override
    public String toString() {
        return "RedisStreamsSourceSplitState{"
                + "split="
                + split
                + ", currentEntryId='"
                + currentEntryId
                + '\''
                + '}';
    }
}
