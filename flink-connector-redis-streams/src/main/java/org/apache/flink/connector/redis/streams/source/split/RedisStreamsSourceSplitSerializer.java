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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * Binary serializer for {@link RedisStreamsSourceSplit}.
 *
 * <p>Version history:
 *
 * <ul>
 *   <li>Version 1: {@code streamKey}, {@code lastReadEntryId}.
 *   <li>Version 2 (current): {@code streamKey}, {@code startingEntryId}, {@code stoppingEntryId}.
 *       {@code lastReadEntryId} from v1 is migrated to {@code startingEntryId}; {@code
 *       stoppingEntryId} defaults to {@code null} (unbounded) on restore.
 * </ul>
 */
@Internal
public final class RedisStreamsSourceSplitSerializer
        implements SimpleVersionedSerializer<RedisStreamsSourceSplit> {

    public static final RedisStreamsSourceSplitSerializer INSTANCE =
            new RedisStreamsSourceSplitSerializer();

    public static final int CURRENT_VERSION = 2;

    private static final int SERIALIZER_INITIAL_CAPACITY = 256;

    private RedisStreamsSourceSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RedisStreamsSourceSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(SERIALIZER_INITIAL_CAPACITY);
        out.writeUTF(split.getStreamKey());
        writeNullable(out, split.getStartingEntryId());
        writeNullable(out, split.getStoppingEntryId());
        return out.getCopyOfBuffer();
    }

    @Override
    public RedisStreamsSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        switch (version) {
            case 1:
                {
                    String streamKey = in.readUTF();
                    String lastReadEntryId = readNullable(in);
                    return new RedisStreamsSourceSplit(streamKey, lastReadEntryId, null);
                }
            case CURRENT_VERSION:
                {
                    String streamKey = in.readUTF();
                    String startingEntryId = readNullable(in);
                    String stoppingEntryId = readNullable(in);
                    return new RedisStreamsSourceSplit(streamKey, startingEntryId, stoppingEntryId);
                }
            default:
                throw new IOException("Unsupported serializer version: " + version);
        }
    }

    private static void writeNullable(DataOutputSerializer out, String value) throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(value);
        }
    }

    private static String readNullable(DataInputDeserializer in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
