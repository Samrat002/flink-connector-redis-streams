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

import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RedisStreamsSourceEnumeratorState} and its serializer. */
class RedisStreamsSourceEnumeratorStateTest {

    static Stream<Set<String>> roundTripCases() {
        return Stream.of(
                Collections.emptySet(),
                Set.of("only-one"),
                Set.of("a", "b", "c", "with:special-chars:42"));
    }

    @ParameterizedTest
    @MethodSource("roundTripCases")
    void serializationRoundTripV2(Set<String> pending) throws IOException {
        RedisStreamsSourceEnumeratorState state = new RedisStreamsSourceEnumeratorState(pending);
        byte[] bytes = RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.serialize(state);
        RedisStreamsSourceEnumeratorState restored =
                RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.deserialize(
                        RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.getVersion(), bytes);

        assertThat(restored.getPendingSplits()).isEqualTo(pending);
    }

    @Test
    void v1PayloadDiscardsLegacyDiscoveredKeys() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeInt(1);
        out.writeUTF("active");
        // legacy "discoveredStreamKeys" field — must be read and discarded
        out.writeInt(2);
        out.writeUTF("active");
        out.writeUTF("discovered-but-unused");

        RedisStreamsSourceEnumeratorState restored =
                RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.deserialize(
                        1, out.getCopyOfBuffer());

        assertThat(restored.getPendingSplits()).containsExactly("active");
    }

    @Test
    void unsupportedVersionRejected() {
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.deserialize(
                                        99, new byte[] {0, 0, 0, 0}))
                .isInstanceOf(IOException.class);
    }

    @Test
    void equalsAndHashCodeReflectPendingSet() {
        Set<String> a = new HashSet<>(Set.of("x", "y"));
        Set<String> b = new HashSet<>(Set.of("y", "x"));
        assertThat(new RedisStreamsSourceEnumeratorState(a))
                .isEqualTo(new RedisStreamsSourceEnumeratorState(b))
                .hasSameHashCodeAs(new RedisStreamsSourceEnumeratorState(b));
    }

    @Test
    void rejectsNullPending() {
        assertThatThrownBy(() -> new RedisStreamsSourceEnumeratorState(null))
                .isInstanceOf(NullPointerException.class);
    }
}
