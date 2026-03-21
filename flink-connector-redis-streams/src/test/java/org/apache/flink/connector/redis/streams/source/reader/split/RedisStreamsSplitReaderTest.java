/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.redis.streams.source.reader.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.redis.streams.source.config.RedisStreamsSourceConfig;
import org.apache.flink.connector.redis.streams.source.config.StartupMode;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.PendingMessages;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link RedisStreamsSplitReader} against a real Redis container.
 *
 * <p>Coverage:
 *
 * <ul>
 *   <li>Backpressure when the deferred-ACK queue is at capacity.
 *   <li>Multi-checkpoint cycle: progressive ACKs across N checkpoints.
 *   <li>Aborted checkpoint: snapshot is discarded, no premature ACK.
 *   <li>PEL recovery: small and larger-than-batch.
 *   <li>Bounded mode termination based on stoppingEntryId.
 *   <li>Out-of-order {@code notifyCheckpointComplete} is dropped.
 *   <li>{@code addSplitsBack} re-enters PEL recovery for the re-assigned split.
 * </ul>
 */
class RedisStreamsSplitReaderTest {

    private static final String REDIS_IMAGE = "redis:7-alpine";
    private static final int REDIS_PORT = 6379;
    private static final String STREAM = "stream-1";
    private static final String CONSUMER_GROUP = "cg";
    private static final String CONSUMER_NAME = "consumer";

    /**
     * Override to point at an externally-managed Redis (host:port) when testcontainers cannot reach
     * the local Docker daemon. Useful for local development on machines whose docker client/daemon
     * is older than the docker-java client baked into testcontainers.
     */
    private static final String EXTERNAL_REDIS = System.getenv("FLINK_REDIS_TEST_HOSTPORT");

    private static GenericContainer<?> REDIS;
    private static String redisHost;
    private static int redisPort;

    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> commands;

    private RedisStreamsSplitReader currentReader;

    @BeforeAll
    static void startBackend() {
        if (EXTERNAL_REDIS != null && !EXTERNAL_REDIS.isEmpty()) {
            String[] hp = EXTERNAL_REDIS.split(":", 2);
            redisHost = hp[0];
            redisPort = Integer.parseInt(hp[1]);
            return;
        }
        try {
            REDIS = new GenericContainer<>(REDIS_IMAGE).withExposedPorts(REDIS_PORT);
            REDIS.start();
            redisHost = REDIS.getHost();
            redisPort = REDIS.getMappedPort(REDIS_PORT);
        } catch (Throwable t) {
            // Skip the entire suite cleanly when no Redis is reachable. Set
            // FLINK_REDIS_TEST_HOSTPORT=host:port to point the tests at an externally-managed
            // Redis (useful when the local Docker daemon API is older than the docker-java
            // client baked into testcontainers).
            REDIS = null;
            Assumptions.abort(
                    "No Redis backend available — set FLINK_REDIS_TEST_HOSTPORT or run a "
                            + "modern Docker daemon. Cause: "
                            + t.getMessage());
        }
    }

    @AfterAll
    static void stopBackend() {
        if (REDIS != null) {
            REDIS.stop();
        }
    }

    @BeforeEach
    void openConnection() {
        client =
                RedisClient.create(
                        RedisURI.builder().withHost(redisHost).withPort(redisPort).build());
        connection = client.connect();
        commands = connection.sync();
        commands.flushdb();
    }

    @AfterEach
    void cleanupReader() throws Exception {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            currentReader = null;
        }
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    private RedisStreamsSourceConfig.Builder cfg() {
        return RedisStreamsSourceConfig.builder()
                .setHost(redisHost)
                .setPort(redisPort)
                .setConsumerGroup(CONSUMER_GROUP)
                .setConsumerName(CONSUMER_NAME)
                .setStreamKeys(List.of(STREAM))
                .setStartupMode(StartupMode.EARLIEST)
                .setPollTimeout(200);
    }

    private RedisStreamsSplitReader newReader(RedisStreamsSourceConfig config) {
        currentReader = new RedisStreamsSplitReader(config, 0);
        return currentReader;
    }

    private static RedisStreamsSourceSplit unboundedSplit() {
        return new RedisStreamsSourceSplit(STREAM);
    }

    private static List<String> idsOf(RecordsWithSplitIds<StreamMessage<String, String>> records) {
        List<String> ids = new ArrayList<>();
        String splitId = records.nextSplit();
        while (splitId != null) {
            StreamMessage<String, String> msg = records.nextRecordFromSplit();
            while (msg != null) {
                ids.add(msg.getId());
                msg = records.nextRecordFromSplit();
            }
            splitId = records.nextSplit();
        }
        return ids;
    }

    private long pelCount() {
        PendingMessages pending = commands.xpending(STREAM, CONSUMER_GROUP);
        return pending == null ? 0 : pending.getCount();
    }

    // -------------------------------------------------------------------------

    @Test
    void backpressureSkipsFetchWhenQueueAtCapacity() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().setMaxDeferredAckQueueSize(2).build());

        commands.xadd(STREAM, Map.of("f", "1"));
        commands.xadd(STREAM, Map.of("f", "2"));

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        idsOf(reader.fetch()); // drain to deferredAcks

        Deque<String> deferredAfterFirst = reader.getDeferredAcksForSplit(STREAM);
        assertThat(deferredAfterFirst).hasSize(2);

        // Add more messages; second fetch must skip because queue is at capacity (2).
        commands.xadd(STREAM, Map.of("f", "3"));
        commands.xadd(STREAM, Map.of("f", "4"));
        idsOf(reader.fetch());

        assertThat(reader.getDeferredAcksForSplit(STREAM))
                .as("queue at capacity blocks further fetches")
                .containsExactlyElementsOf(deferredAfterFirst);
    }

    @Test
    void multipleCheckpointCyclesAckProgressively() throws Exception {
        // The bug this guards against: the previous "delta watermark" implementation broke after
        // 2-3 cycles because lastCompleted exceeded queue size; new ACKs went negative and PEL
        // grew unbounded. Three full cycles must each successfully drain the FIFO front.
        RedisStreamsSplitReader reader = newReader(cfg().setMaxDeferredAckQueueSize(100).build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        // Cycle 1: 2 messages.
        commands.xadd(STREAM, Map.of("k", "a1"));
        commands.xadd(STREAM, Map.of("k", "a2"));
        idsOf(reader.fetch());
        reader.markCheckpoint(1L);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(1L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).isEmpty();
        assertThat(pelCount()).isZero();

        // Cycle 2: 3 messages.
        commands.xadd(STREAM, Map.of("k", "b1"));
        commands.xadd(STREAM, Map.of("k", "b2"));
        commands.xadd(STREAM, Map.of("k", "b3"));
        idsOf(reader.fetch());
        reader.markCheckpoint(2L);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(2L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).isEmpty();
        assertThat(pelCount()).isZero();

        // Cycle 3: 5 messages — was guaranteed to fail under the old (delta watermark) logic
        // because lastCompleted (5) > snapshot (5) producing limit=0.
        for (int i = 0; i < 5; i++) {
            commands.xadd(STREAM, Map.of("k", "c" + i));
        }
        idsOf(reader.fetch());
        reader.markCheckpoint(3L);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(3L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).isEmpty();
        assertThat(pelCount()).as("PEL fully drained after every checkpoint").isZero();
    }

    @Test
    void messagesArrivingAfterBarrierAreNotAckedUntilNextCheckpoint() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        commands.xadd(STREAM, Map.of("k", "barrier-1"));
        commands.xadd(STREAM, Map.of("k", "barrier-2"));
        idsOf(reader.fetch());
        reader.markCheckpoint(1L); // captures size = 2

        // After the barrier, fetch a third message. It must NOT be acked at notify(1).
        commands.xadd(STREAM, Map.of("k", "post-barrier"));
        idsOf(reader.fetch());

        reader.acknowledgeAllPendingMessagesAtCheckpoint(1L);

        Deque<String> remaining = reader.getDeferredAcksForSplit(STREAM);
        assertThat(remaining).hasSize(1); // only the post-barrier id remains
    }

    @Test
    void abortedCheckpointDoesNotAck() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        commands.xadd(STREAM, Map.of("k", "x"));
        commands.xadd(STREAM, Map.of("k", "y"));
        idsOf(reader.fetch());

        reader.markCheckpoint(10L);
        reader.discardCheckpoint(10L);

        // After abort, calling notifyCheckpointComplete with the discarded id is a no-op.
        reader.acknowledgeAllPendingMessagesAtCheckpoint(10L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).hasSize(2);
        assertThat(pelCount()).isEqualTo(2);
    }

    @Test
    void outOfOrderNotifyCheckpointCompleteIsDropped() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        commands.xadd(STREAM, Map.of("k", "1"));
        commands.xadd(STREAM, Map.of("k", "2"));
        idsOf(reader.fetch());

        reader.markCheckpoint(5L);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(5L);
        assertThat(reader.getMaxCommittedCheckpointId()).isEqualTo(5L);

        // A late callback for an older checkpoint must not re-process or revert state.
        reader.markCheckpoint(3L);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(3L);
        assertThat(reader.getMaxCommittedCheckpointId())
                .as("maxCommitted only advances forward")
                .isEqualTo(5L);
    }

    @Test
    void pelRecoverySmallBatch() throws Exception {
        // Reader A fetches 2 messages but does not ACK before close — entries remain in PEL.
        RedisStreamsSplitReader readerA = newReader(cfg().build());
        commands.xadd(STREAM, Map.of("k", "v1"));
        commands.xadd(STREAM, Map.of("k", "v2"));
        readerA.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> firstFetch = idsOf(readerA.fetch());
        assertThat(firstFetch).hasSize(2);
        assertThat(pelCount()).isEqualTo(2);
        readerA.close();
        currentReader = null;

        // Reader B with the same consumer name MUST recover the PEL, then ACK at checkpoint.
        RedisStreamsSplitReader readerB = newReader(cfg().build());
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> recovered = idsOf(readerB.fetch());
        assertThat(recovered).containsExactlyElementsOf(firstFetch);

        // Add a new message; it should NOT be returned in the same fetch (recovery completed).
        commands.xadd(STREAM, Map.of("k", "v3"));
        List<String> steady = idsOf(readerB.fetch());
        assertThat(steady).hasSize(1).doesNotContainAnyElementsOf(firstFetch);

        // Drive a checkpoint and verify XACK happened.
        readerB.markCheckpoint(1L);
        readerB.acknowledgeAllPendingMessagesAtCheckpoint(1L);
        assertThat(pelCount()).as("PEL fully drained after ACK").isZero();
    }

    @Test
    void pelRecoveryLargerThanBatch() throws Exception {
        int total = 250;
        int batch = 100;

        RedisStreamsSplitReader readerA = newReader(cfg().setBatchSize(batch).build());
        for (int i = 0; i < total; i++) {
            commands.xadd(STREAM, Map.of("k", String.valueOf(i)));
        }
        readerA.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> all = new ArrayList<>();
        // Drain via repeated fetches.
        for (int i = 0; i < 5 && all.size() < total; i++) {
            all.addAll(idsOf(readerA.fetch()));
        }
        assertThat(all).hasSize(total);
        assertThat(pelCount()).isEqualTo(total);
        readerA.close();
        currentReader = null;

        RedisStreamsSplitReader readerB = newReader(cfg().setBatchSize(batch).build());
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> recovered = new ArrayList<>();
        for (int i = 0; i < 10 && recovered.size() < total; i++) {
            recovered.addAll(idsOf(readerB.fetch()));
        }
        assertThat(recovered)
                .as("recovery returns every PEL entry exactly once")
                .containsExactlyElementsOf(all);

        readerB.markCheckpoint(1L);
        readerB.acknowledgeAllPendingMessagesAtCheckpoint(1L);
        assertThat(pelCount()).isZero();
    }

    @Test
    void boundedSplitFinishesAtStoppingId() throws Exception {
        // Pre-populate 3 entries and capture the stopping bound.
        commands.xadd(STREAM, Map.of("k", "a"));
        String stoppingId = commands.xadd(STREAM, Map.of("k", "b"));
        commands.xadd(STREAM, Map.of("k", "c")); // appended AFTER bound; must NOT be consumed

        RedisStreamsSplitReader reader = newReader(cfg().setBounded(true).build());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(new RedisStreamsSourceSplit(STREAM, null, stoppingId))));

        // Repeatedly fetch until the split is reported finished.
        List<String> consumed = new ArrayList<>();
        boolean finished = false;
        for (int i = 0; i < 5 && !finished; i++) {
            RecordsWithSplitIds<StreamMessage<String, String>> records = reader.fetch();
            consumed.addAll(idsOf(records));
            finished = records.finishedSplits().contains(STREAM);
        }
        assertThat(finished).as("split reaches finished state at stopping bound").isTrue();
        assertThat(consumed)
                .as("only entries up to and including stopping id are consumed")
                .hasSize(2)
                .last()
                .isEqualTo(stoppingId);
    }

    @Test
    void deferredAcksForFinishedSplitDrainAtNextCheckpoint() throws Exception {
        // Bounded split where stopping id == first entry, plus a second entry beyond bound.
        String stoppingId = commands.xadd(STREAM, Map.of("k", "a"));
        commands.xadd(STREAM, Map.of("k", "b"));

        RedisStreamsSplitReader reader = newReader(cfg().setBounded(true).build());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(new RedisStreamsSourceSplit(STREAM, null, stoppingId))));

        boolean finished = false;
        for (int i = 0; i < 5 && !finished; i++) {
            RecordsWithSplitIds<StreamMessage<String, String>> r = reader.fetch();
            idsOf(r);
            finished = r.finishedSplits().contains(STREAM);
        }
        assertThat(finished).isTrue();
        assertThat(pelCount()).as("entry was fetched but not yet acked").isEqualTo(1);

        reader.markCheckpoint(1L);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(1L);

        assertThat(pelCount()).as("finished-split ACKs drain at checkpoint").isZero();
    }

    @Test
    void addSplitsBackReentersPelRecovery() throws Exception {
        // Reader A leaves 1 unacked entry in the PEL, then close.
        RedisStreamsSplitReader readerA = newReader(cfg().build());
        commands.xadd(STREAM, Map.of("k", "stale"));
        readerA.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        idsOf(readerA.fetch());
        assertThat(pelCount()).isEqualTo(1);
        readerA.close();
        currentReader = null;

        // Reader B reassigns the same split — must drain PEL before reading new entries.
        RedisStreamsSplitReader readerB = newReader(cfg().build());
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        // Re-add the split (simulating addSplitsBack from the enumerator). The reader must
        // re-enter PEL recovery for it.
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        List<String> recovered = idsOf(readerB.fetch());
        assertThat(recovered).as("re-assignment re-reads the existing PEL entry").hasSize(1);
    }

    @Test
    void compareEntryIdsOrdersByMillisThenSequence() {
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-0", "100-0")).isZero();
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-0", "100-1")).isNegative();
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-2", "100-1")).isPositive();
        assertThat(RedisStreamsSplitReader.compareEntryIds("99-9", "100-0")).isNegative();
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-0", "99-9999")).isPositive();
        // Single-component (no dash) should still compare numerically with seq=0.
        assertThat(RedisStreamsSplitReader.compareEntryIds("100", "100-0")).isZero();
    }
}
