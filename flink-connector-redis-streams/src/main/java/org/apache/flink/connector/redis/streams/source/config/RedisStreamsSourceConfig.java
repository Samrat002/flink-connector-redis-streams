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

package org.apache.flink.connector.redis.streams.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Configuration for the Redis Streams Source.
 *
 * <h3>Consumer name and rescaling</h3>
 *
 * <p>If {@code consumerName} is not set, a UUID-suffixed default is generated <em>once on the job
 * client</em> and embedded in the serialized config. The effective per-subtask consumer name is
 * {@code <consumerName>-<subtaskIndex>}. Two consequences worth knowing:
 *
 * <ul>
 *   <li><b>Rescaling</b> changes subtask indices, so a parallelism change orphans PEL entries under
 *       the old subtask-suffixed consumer names. To preserve PEL recovery across rescale, pin
 *       {@code consumerName} explicitly and avoid changing parallelism without draining.
 *   <li><b>Code redeploy without state restore</b> generates a new UUID, which similarly orphans
 *       PEL entries from the previous run.
 * </ul>
 */
@PublicEvolving
public class RedisStreamsSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CONSUMER_GROUP = "flink-consumer-group";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_DATABASE = 0;
    public static final long DEFAULT_POLL_TIMEOUT_MS = 1000L;
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_MAX_DEFERRED_ACK_QUEUE_SIZE = 10_000;
    public static final long DEFAULT_CIRCUIT_BREAKER_OPEN_DURATION_MS = 5_000L;
    public static final int DEFAULT_CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3;

    private static final String CONSUMER_NAME_PREFIX = "flink-consumer-";
    private static final int CONSUMER_NAME_SUFFIX_LENGTH = 8;

    private final String host;
    private final int port;
    private final String password;
    private final int database;
    private final List<String> streamKeys;
    private final String consumerGroup;
    private final String consumerName;
    private final boolean bounded;
    private final long pollTimeout;
    private final int batchSize;
    private final StartupMode startupMode;
    private final int maxDeferredAckQueueSize;
    private final long circuitBreakerOpenDurationMs;
    private final int circuitBreakerFailureThreshold;

    private RedisStreamsSourceConfig(
            String host,
            int port,
            String password,
            int database,
            List<String> streamKeys,
            String consumerGroup,
            String consumerName,
            boolean bounded,
            long pollTimeout,
            int batchSize,
            StartupMode startupMode,
            int maxDeferredAckQueueSize,
            long circuitBreakerOpenDurationMs,
            int circuitBreakerFailureThreshold) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.database = database;
        this.streamKeys = Collections.unmodifiableList(new ArrayList<>(streamKeys));
        this.consumerGroup = consumerGroup;
        this.consumerName = consumerName;
        this.bounded = bounded;
        this.pollTimeout = pollTimeout;
        this.batchSize = batchSize;
        this.startupMode = startupMode;
        this.maxDeferredAckQueueSize = maxDeferredAckQueueSize;
        this.circuitBreakerOpenDurationMs = circuitBreakerOpenDurationMs;
        this.circuitBreakerFailureThreshold = circuitBreakerFailureThreshold;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPassword() {
        return password;
    }

    public int getDatabase() {
        return database;
    }

    public List<String> getStreamKeys() {
        return streamKeys;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public boolean isBounded() {
        return bounded;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public StartupMode getStartupMode() {
        return startupMode;
    }

    public int getMaxDeferredAckQueueSize() {
        return maxDeferredAckQueueSize;
    }

    public long getCircuitBreakerOpenDurationMs() {
        return circuitBreakerOpenDurationMs;
    }

    public int getCircuitBreakerFailureThreshold() {
        return circuitBreakerFailureThreshold;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link RedisStreamsSourceConfig}. */
    public static class Builder {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private String password = null;
        private int database = DEFAULT_DATABASE;
        private List<String> streamKeys;
        private String consumerGroup = DEFAULT_CONSUMER_GROUP;
        private String consumerName;
        private boolean bounded = false;
        private long pollTimeout = DEFAULT_POLL_TIMEOUT_MS;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private StartupMode startupMode = StartupMode.LATEST;
        private int maxDeferredAckQueueSize = DEFAULT_MAX_DEFERRED_ACK_QUEUE_SIZE;
        private long circuitBreakerOpenDurationMs = DEFAULT_CIRCUIT_BREAKER_OPEN_DURATION_MS;
        private int circuitBreakerFailureThreshold = DEFAULT_CIRCUIT_BREAKER_FAILURE_THRESHOLD;

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public Builder setStreamKeys(List<String> streamKeys) {
            this.streamKeys = streamKeys;
            return this;
        }

        public Builder setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
        }

        public Builder setConsumerName(String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        public Builder setBounded(boolean bounded) {
            this.bounded = bounded;
            return this;
        }

        public Builder setPollTimeout(long pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setStartupMode(StartupMode startupMode) {
            this.startupMode = startupMode;
            return this;
        }

        public Builder setMaxDeferredAckQueueSize(int maxDeferredAckQueueSize) {
            this.maxDeferredAckQueueSize = maxDeferredAckQueueSize;
            return this;
        }

        public Builder setCircuitBreakerOpenDurationMs(long circuitBreakerOpenDurationMs) {
            this.circuitBreakerOpenDurationMs = circuitBreakerOpenDurationMs;
            return this;
        }

        public Builder setCircuitBreakerFailureThreshold(int circuitBreakerFailureThreshold) {
            this.circuitBreakerFailureThreshold = circuitBreakerFailureThreshold;
            return this;
        }

        public RedisStreamsSourceConfig build() {
            Preconditions.checkNotNull(host, "host must be set");
            Preconditions.checkArgument(!host.isEmpty(), "host must not be empty");
            Preconditions.checkArgument(port > 0 && port <= 65535, "port must be in 1..65535");
            Preconditions.checkArgument(database >= 0, "database must be non-negative");
            Preconditions.checkNotNull(streamKeys, "streamKeys must be set");
            Preconditions.checkArgument(!streamKeys.isEmpty(), "streamKeys must not be empty");
            for (String key : streamKeys) {
                Preconditions.checkArgument(
                        key != null && !key.isEmpty(),
                        "streamKeys must not contain null or empty entries");
            }
            Preconditions.checkNotNull(consumerGroup, "consumerGroup must be set");
            Preconditions.checkArgument(
                    !consumerGroup.isEmpty(), "consumerGroup must not be empty");
            Preconditions.checkNotNull(startupMode, "startupMode must be set");
            Preconditions.checkArgument(batchSize > 0, "batchSize must be positive");
            Preconditions.checkArgument(pollTimeout > 0, "pollTimeout must be positive");
            Preconditions.checkArgument(
                    maxDeferredAckQueueSize > 0, "maxDeferredAckQueueSize must be positive");
            Preconditions.checkArgument(
                    circuitBreakerFailureThreshold > 0,
                    "circuitBreakerFailureThreshold must be positive");
            Preconditions.checkArgument(
                    circuitBreakerOpenDurationMs > 0,
                    "circuitBreakerOpenDurationMs must be positive");

            if (consumerName == null) {
                consumerName =
                        CONSUMER_NAME_PREFIX
                                + UUID.randomUUID()
                                        .toString()
                                        .substring(0, CONSUMER_NAME_SUFFIX_LENGTH);
            } else {
                Preconditions.checkArgument(
                        !consumerName.isEmpty(), "consumerName must not be empty");
            }

            return new RedisStreamsSourceConfig(
                    host,
                    port,
                    password,
                    database,
                    streamKeys,
                    consumerGroup,
                    consumerName,
                    bounded,
                    pollTimeout,
                    batchSize,
                    startupMode,
                    maxDeferredAckQueueSize,
                    circuitBreakerOpenDurationMs,
                    circuitBreakerFailureThreshold);
        }
    }
}
