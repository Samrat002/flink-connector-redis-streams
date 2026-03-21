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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Binary serializer for {@link RedisStreamsSourceEnumeratorState}.
 *
 * <p>Version history:
 *
 * <ul>
 *   <li>Version 1: serialized {@code pendingSplits} + {@code discoveredStreamKeys}. {@code
 *       discoveredStreamKeys} was dead state (dynamic discovery was never implemented) and has been
 *       removed.
 *   <li>Version 2 (current): serializes only {@code pendingSplits}. Version 1 checkpoints are read
 *       correctly — the {@code discoveredStreamKeys} field is consumed and discarded.
 * </ul>
 */
@Internal
public class RedisStreamsSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<RedisStreamsSourceEnumeratorState> {

    public static final RedisStreamsSourceEnumeratorStateSerializer INSTANCE =
            new RedisStreamsSourceEnumeratorStateSerializer();

    public static final int CURRENT_VERSION = 2;

    private static final int SERIALIZER_INITIAL_CAPACITY = 128;

    private RedisStreamsSourceEnumeratorStateSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RedisStreamsSourceEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(SERIALIZER_INITIAL_CAPACITY);
        Set<String> pendingSplits = state.getPendingSplits();
        out.writeInt(pendingSplits.size());
        for (String split : pendingSplits) {
            out.writeUTF(split);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public RedisStreamsSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        int pendingSize = in.readInt();
        Set<String> pendingSplits = new HashSet<>(pendingSize);
        for (int i = 0; i < pendingSize; i++) {
            pendingSplits.add(in.readUTF());
        }

        if (version == 1) {
            // Version 1 had a discoveredStreamKeys field that is now removed.
            // Read and discard it for backward compatibility.
            int discoveredSize = in.readInt();
            for (int i = 0; i < discoveredSize; i++) {
                in.readUTF();
            }
        } else if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported serializer version: " + version);
        }

        return new RedisStreamsSourceEnumeratorState(pendingSplits);
    }
}
