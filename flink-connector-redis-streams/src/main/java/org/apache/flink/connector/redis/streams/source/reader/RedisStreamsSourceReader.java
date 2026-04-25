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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.redis.streams.source.RedisStreamsDeserializationSchema;
import org.apache.flink.connector.redis.streams.source.reader.split.RedisStreamsSplitReader;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplitState;
import org.apache.flink.util.Preconditions;

import io.lettuce.core.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Source reader for Redis Streams. */
@Internal
public class RedisStreamsSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                StreamMessage<String, String>,
                T,
                RedisStreamsSourceSplit,
                RedisStreamsSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamsSourceReader.class);

    private final AtomicReference<RedisStreamsSplitReader> splitReaderRef;

    public RedisStreamsSourceReader(
            SingleThreadFetcherManager<StreamMessage<String, String>, RedisStreamsSourceSplit>
                    splitFetcherManager,
            RedisStreamsDeserializationSchema<T> deserializationSchema,
            AtomicReference<RedisStreamsSplitReader> splitReaderRef,
            Configuration config,
            SourceReaderContext context) {
        super(
                splitFetcherManager,
                new RedisStreamsRecordEmitter<>(deserializationSchema),
                config,
                context);
        this.splitReaderRef = Preconditions.checkNotNull(splitReaderRef);

        LOG.info(
                "RedisStreamsSourceReader initialized for subtask {}", context.getIndexOfSubtask());
    }

    @Override
    protected void onSplitFinished(Map<String, RedisStreamsSourceSplitState> finishedSplitIds) {
        LOG.info("Splits finished: {}", finishedSplitIds.keySet());
    }

    @Override
    protected RedisStreamsSourceSplitState initializedState(RedisStreamsSourceSplit split) {
        return new RedisStreamsSourceSplitState(split);
    }

    @Override
    protected RedisStreamsSourceSplit toSplitType(
            String splitId, RedisStreamsSourceSplitState splitState) {
        return splitState.toSplit();
    }

    @Override
    public List<RedisStreamsSourceSplit> snapshotState(long checkpointId) {
        List<RedisStreamsSourceSplit> splits = super.snapshotState(checkpointId);
        RedisStreamsSplitReader reader = splitReaderRef.get();
        if (reader != null) {
            reader.markCheckpoint(checkpointId);
        }
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        RedisStreamsSplitReader reader = splitReaderRef.get();
        if (reader != null) {
            reader.acknowledgeAllPendingMessagesAtCheckpoint(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        RedisStreamsSplitReader reader = splitReaderRef.get();
        if (reader != null) {
            reader.discardCheckpoint(checkpointId);
        }
        super.notifyCheckpointAborted(checkpointId);
    }
}
