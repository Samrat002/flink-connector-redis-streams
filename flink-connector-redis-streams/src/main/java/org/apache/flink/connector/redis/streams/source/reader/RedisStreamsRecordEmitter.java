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

package org.apache.flink.connector.redis.streams.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.redis.streams.source.RedisStreamsDeserializationSchema;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplitState;

import io.lettuce.core.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Record emitter that deserializes Redis StreamMessages. */
@Internal
public class RedisStreamsRecordEmitter<T>
        implements RecordEmitter<StreamMessage<String, String>, T, RedisStreamsSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamsRecordEmitter.class);

    private final RedisStreamsDeserializationSchema<T> deserializationSchema;

    public RedisStreamsRecordEmitter(RedisStreamsDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            StreamMessage<String, String> element,
            SourceOutput<T> output,
            RedisStreamsSourceSplitState splitState)
            throws Exception {
        try {
            T record =
                    deserializationSchema.deserialize(
                            element.getStream(), element.getId(), element.getBody());

            // Always advance the split offset regardless of whether the record is null
            // (filtered). Failing to do so causes null-filtered messages to remain in the PEL
            // indefinitely — they will be re-delivered on every recovery and re-filtered,
            // creating unbounded PEL growth.
            splitState.setCurrentEntryId(element.getId());

            if (record != null) {
                output.collect(record);
                LOG.debug(
                        "Emitted record from split {} with entry ID {}",
                        splitState.getStreamKey(),
                        element.getId());
            } else {
                LOG.debug("Deserialization returned null for entry ID {}", element.getId());
            }
        } catch (Exception e) {
            LOG.error(
                    "Failed to deserialize record from stream {} with ID {}",
                    element.getStream(),
                    element.getId(),
                    e);
            throw e;
        }
    }
}
