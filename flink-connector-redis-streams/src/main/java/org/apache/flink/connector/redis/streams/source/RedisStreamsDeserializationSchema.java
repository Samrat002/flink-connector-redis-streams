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

package org.apache.flink.connector.redis.streams.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.Map;

/**
 * Deserialization schema for Redis Stream entries.
 *
 * <p>This interface defines how Redis Stream entries (field-value pairs) are converted into records
 * of type {@code T}. Implementations must be {@link Serializable} to support distributed
 * processing.
 *
 * <p>Example implementation:
 *
 * <pre>{@code
 * public class MyDeserializer implements RedisStreamsDeserializationSchema<MyPojo> {
 *     @Override
 *     public MyPojo deserialize(String streamKey, String entryId, Map<String, String> fields) {
 *         return new MyPojo(
 *             fields.get("field1"),
 *             fields.get("field2")
 *         );
 *     }
 *
 *     @Override
 *     public TypeInformation<MyPojo> getProducedType() {
 *         return TypeInformation.of(MyPojo.class);
 *     }
 * }
 * }</pre>
 *
 * <p>This interface is part of the Public API and is stable across versions.
 *
 * @param <T> The type of the deserialized record
 */
@PublicEvolving
public interface RedisStreamsDeserializationSchema<T> extends Serializable {

    /**
     * Deserializes a Redis Stream entry into a record.
     *
     * @param streamKey The Redis Stream key from which this entry was read
     * @param entryId The unique entry ID in the stream (format: "timestamp-sequence")
     * @param fields The field-value pairs in the stream entry (Redis Stream message body)
     * @return The deserialized record
     * @throws Exception If deserialization fails
     */
    T deserialize(String streamKey, String entryId, Map<String, String> fields) throws Exception;

    /**
     * Gets the type information for the produced type.
     *
     * @return The type information for type {@code T}
     */
    TypeInformation<T> getProducedType();
}
