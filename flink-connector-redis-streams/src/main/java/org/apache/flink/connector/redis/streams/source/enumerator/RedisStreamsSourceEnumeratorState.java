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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Checkpointed enumerator state: unassigned stream keys + frozen bounded-mode stopping IDs. */
@Internal
public class RedisStreamsSourceEnumeratorState {

    private final Set<String> pendingSplits;
    private final Map<String, String> stoppingEntryIds;

    public RedisStreamsSourceEnumeratorState(Set<String> pendingSplits) {
        this(pendingSplits, Collections.emptyMap());
    }

    public RedisStreamsSourceEnumeratorState(
            Set<String> pendingSplits, Map<String, String> stoppingEntryIds) {
        this.pendingSplits = Objects.requireNonNull(pendingSplits);
        this.stoppingEntryIds = Objects.requireNonNull(stoppingEntryIds);
    }

    public Set<String> getPendingSplits() {
        return pendingSplits;
    }

    public Map<String, String> getStoppingEntryIds() {
        return stoppingEntryIds;
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
        return Objects.equals(pendingSplits, that.pendingSplits)
                && Objects.equals(stoppingEntryIds, that.stoppingEntryIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pendingSplits, stoppingEntryIds);
    }
}
