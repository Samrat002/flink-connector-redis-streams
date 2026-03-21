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

package org.apache.flink.connector.redis.streams.source.enumerator;

import org.apache.flink.annotation.Internal;

import java.util.Objects;
import java.util.Set;

/**
 * Checkpointed state of the Redis Streams Source Enumerator.
 *
 * <p>Contains only {@code pendingSplits} — stream keys that have been discovered but not yet
 * assigned to a reader. Stream keys that are already assigned are tracked by the readers themselves
 * and do not need to be snapshotted here.
 */
@Internal
public class RedisStreamsSourceEnumeratorState {

    private final Set<String> pendingSplits;

    public RedisStreamsSourceEnumeratorState(Set<String> pendingSplits) {
        this.pendingSplits = Objects.requireNonNull(pendingSplits);
    }

    public Set<String> getPendingSplits() {
        return pendingSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisStreamsSourceEnumeratorState that = (RedisStreamsSourceEnumeratorState) o;
        return Objects.equals(pendingSplits, that.pendingSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pendingSplits);
    }
}
