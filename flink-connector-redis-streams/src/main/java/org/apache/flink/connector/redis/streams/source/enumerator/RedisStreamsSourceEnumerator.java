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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.redis.streams.source.config.RedisStreamsSourceConfig;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Enumerator for the Redis Streams Source.
 *
 * <p>One split per stream key, distributed round-robin across registered readers. For bounded
 * sources, the stopping entry ID for each stream is captured once at startup via {@code XINFO
 * STREAM} and frozen onto the split — readers stop the split when they consume past it.
 *
 * <p>Synchronization: mutable state is guarded by {@code synchronized(this)}. Calls into the {@link
 * SplitEnumeratorContext} are made <em>outside</em> the lock to avoid deadlocks; the context may
 * dispatch internally.
 */
@Internal
public class RedisStreamsSourceEnumerator
        implements SplitEnumerator<RedisStreamsSourceSplit, RedisStreamsSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamsSourceEnumerator.class);

    private final RedisStreamsSourceConfig sourceConfig;
    private final SplitEnumeratorContext<RedisStreamsSourceSplit> context;

    /** Stream keys not yet assigned to a reader. */
    private final Set<String> pendingSplitKeys;

    /** Per-reader assignments, for {@code addSplitsBack} accounting. */
    private final Map<Integer, Set<String>> readerAssignments = new HashMap<>();

    /** Readers we've already signaled no-more-splits to (bounded mode only). */
    private final Set<Integer> readersSignaledNoMoreSplits = new HashSet<>();

    /**
     * Stopping entry IDs per stream key (bounded mode only). Captured once at start; if the stream
     * does not exist yet, the stopping ID is "0-0" so a fresh stream finishes immediately in
     * bounded mode (consistent semantics).
     */
    private final Map<String, String> stoppingEntryIds = new HashMap<>();

    /**
     * Resolves the latest entry ID for a stream key. In production this opens a Redis connection;
     * tests can inject a stub.
     */
    private final Function<String, String> lastGeneratedIdLookup;

    public RedisStreamsSourceEnumerator(
            RedisStreamsSourceConfig sourceConfig,
            SplitEnumeratorContext<RedisStreamsSourceSplit> context,
            @Nullable RedisStreamsSourceEnumeratorState restoredState) {
        this(sourceConfig, context, restoredState, defaultLookup(sourceConfig));
    }

    @VisibleForTesting
    RedisStreamsSourceEnumerator(
            RedisStreamsSourceConfig sourceConfig,
            SplitEnumeratorContext<RedisStreamsSourceSplit> context,
            @Nullable RedisStreamsSourceEnumeratorState restoredState,
            Function<String, String> lastGeneratedIdLookup) {
        this.sourceConfig = sourceConfig;
        this.context = context;
        this.lastGeneratedIdLookup = lastGeneratedIdLookup;
        this.pendingSplitKeys = new HashSet<>();
        if (restoredState != null) {
            this.pendingSplitKeys.addAll(restoredState.getPendingSplits());
        } else {
            this.pendingSplitKeys.addAll(sourceConfig.getStreamKeys());
        }
    }

    @Override
    public void start() {
        LOG.info("Starting Redis Streams Source Enumerator (bounded={})", sourceConfig.isBounded());
        if (sourceConfig.isBounded()) {
            // Snapshot stopping IDs once. Readers freeze on these; new entries appended after
            // job start are NOT consumed by a bounded job.
            for (String streamKey : sourceConfig.getStreamKeys()) {
                String stoppingId = lastGeneratedIdLookup.apply(streamKey);
                stoppingEntryIds.put(streamKey, stoppingId);
                LOG.info("Bounded mode: stream {} stopping entry id = {}", streamKey, stoppingId);
            }
        }
        dispatchAssignments(buildAssignments());
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Received split request from reader {}", subtaskId);
        dispatchAssignments(buildAssignments());
    }

    @Override
    public void addSplitsBack(List<RedisStreamsSourceSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from reader {}", splits.size(), subtaskId);
        synchronized (this) {
            Set<String> assigned = readerAssignments.get(subtaskId);
            if (assigned != null) {
                for (RedisStreamsSourceSplit split : splits) {
                    assigned.remove(split.splitId());
                }
                if (assigned.isEmpty()) {
                    readerAssignments.remove(subtaskId);
                }
            }
            readersSignaledNoMoreSplits.remove(subtaskId);
            for (RedisStreamsSourceSplit split : splits) {
                pendingSplitKeys.add(split.getStreamKey());
            }
        }
        dispatchAssignments(buildAssignments());
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader {}", subtaskId);
        dispatchAssignments(buildAssignments());
        boolean shouldSignal;
        synchronized (this) {
            shouldSignal =
                    sourceConfig.isBounded()
                            && pendingSplitKeys.isEmpty()
                            && !readersSignaledNoMoreSplits.contains(subtaskId);
            if (shouldSignal) {
                readersSignaledNoMoreSplits.add(subtaskId);
            }
        }
        if (shouldSignal) {
            context.signalNoMoreSplits(subtaskId);
            LOG.info("Signaled no more splits for reader {}", subtaskId);
        }
    }

    @Override
    public RedisStreamsSourceEnumeratorState snapshotState(long checkpointId) {
        LOG.debug("Snapshotting state for checkpoint {}", checkpointId);
        synchronized (this) {
            return new RedisStreamsSourceEnumeratorState(new HashSet<>(pendingSplitKeys));
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing Redis Streams Source Enumerator");
    }

    /**
     * Build a snapshot assignment map under the lock, then return it so the caller can dispatch
     * outside the lock — {@code context.assignSplits} must not be called while holding this lock.
     */
    private Map<Integer, List<RedisStreamsSourceSplit>> buildAssignments() {
        Map<Integer, List<RedisStreamsSourceSplit>> assignments;
        synchronized (this) {
            if (pendingSplitKeys.isEmpty()) {
                return Collections.emptyMap();
            }
            List<Integer> readers = new ArrayList<>(context.registeredReaders().keySet());
            if (readers.isEmpty()) {
                LOG.debug("No readers available for split assignment");
                return Collections.emptyMap();
            }
            assignments = new HashMap<>();
            int idx = 0;
            for (String streamKey : new ArrayList<>(pendingSplitKeys)) {
                int readerId = readers.get(idx % readers.size());
                String stoppingId =
                        sourceConfig.isBounded() ? stoppingEntryIds.get(streamKey) : null;
                RedisStreamsSourceSplit split =
                        new RedisStreamsSourceSplit(streamKey, null, stoppingId);
                assignments.computeIfAbsent(readerId, k -> new ArrayList<>()).add(split);
                readerAssignments.computeIfAbsent(readerId, k -> new HashSet<>()).add(streamKey);
                pendingSplitKeys.remove(streamKey);
                idx++;
            }
        }
        return assignments;
    }

    private void dispatchAssignments(Map<Integer, List<RedisStreamsSourceSplit>> assignments) {
        for (Map.Entry<Integer, List<RedisStreamsSourceSplit>> entry : assignments.entrySet()) {
            int readerId = entry.getKey();
            List<RedisStreamsSourceSplit> splits = entry.getValue();
            LOG.info("Assigning {} splits to reader {}: {}", splits.size(), readerId, splits);
            context.assignSplits(new SplitsAssignment<>(Map.of(readerId, splits)));

            boolean shouldSignal;
            synchronized (this) {
                shouldSignal =
                        sourceConfig.isBounded()
                                && pendingSplitKeys.isEmpty()
                                && !readersSignaledNoMoreSplits.contains(readerId);
                if (shouldSignal) {
                    readersSignaledNoMoreSplits.add(readerId);
                }
            }
            if (shouldSignal) {
                context.signalNoMoreSplits(readerId);
                LOG.info("Signaled no more splits for reader {}", readerId);
            }
        }
    }

    /**
     * Default lookup that opens a short-lived Lettuce client to read {@code XINFO STREAM <key>}'s
     * {@code last-generated-id}. Returns {@code "0-0"} if the stream does not exist yet (so a
     * bounded job over a freshly created stream finishes immediately).
     */
    private static Function<String, String> defaultLookup(RedisStreamsSourceConfig cfg) {
        return streamKey -> {
            RedisURI.Builder uriBuilder =
                    RedisURI.builder()
                            .withHost(cfg.getHost())
                            .withPort(cfg.getPort())
                            .withDatabase(cfg.getDatabase())
                            .withTimeout(Duration.ofSeconds(5));
            if (cfg.getPassword() != null && !cfg.getPassword().isEmpty()) {
                uriBuilder.withPassword(cfg.getPassword().toCharArray());
            }
            try (RedisClient client = RedisClient.create(uriBuilder.build())) {
                client.setOptions(ClientOptions.builder().autoReconnect(false).build());
                try (StatefulRedisConnection<String, String> conn = client.connect()) {
                    RedisCommands<String, String> cmds = conn.sync();
                    try {
                        List<Object> info = cmds.xinfoStream(streamKey);
                        return extractLastGeneratedId(info);
                    } catch (Exception e) {
                        // Stream does not exist or XINFO failed; bound at 0-0.
                        LOG.warn(
                                "Could not read XINFO STREAM for {}; treating as empty. Cause: {}",
                                streamKey,
                                e.getMessage());
                        return "0-0";
                    }
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to query XINFO STREAM for bounded mode setup of " + streamKey, e);
            }
        };
    }

    /**
     * Extract {@code last-generated-id} from the {@code XINFO STREAM} response. Lettuce returns a
     * flat alternating list of {@code key, value, key, value, ...}.
     */
    @VisibleForTesting
    static String extractLastGeneratedId(List<Object> info) {
        if (info == null) {
            return "0-0";
        }
        for (int i = 0; i + 1 < info.size(); i += 2) {
            Object key = info.get(i);
            if (key != null && "last-generated-id".equals(key.toString())) {
                Object val = info.get(i + 1);
                return val == null ? "0-0" : val.toString();
            }
        }
        return "0-0";
    }
}
