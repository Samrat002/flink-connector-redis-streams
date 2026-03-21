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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RedisStreamsSourceConfig}. */
class RedisStreamsSourceConfigTest {

    @Test
    void defaultsAreApplied() {
        RedisStreamsSourceConfig config =
                RedisStreamsSourceConfig.builder().setStreamKeys(List.of("s")).build();

        assertThat(config.getHost()).isEqualTo(RedisStreamsSourceConfig.DEFAULT_HOST);
        assertThat(config.getPort()).isEqualTo(RedisStreamsSourceConfig.DEFAULT_PORT);
        assertThat(config.getDatabase()).isEqualTo(RedisStreamsSourceConfig.DEFAULT_DATABASE);
        assertThat(config.getPassword()).isNull();
        assertThat(config.getStreamKeys()).containsExactly("s");
        assertThat(config.getConsumerGroup())
                .isEqualTo(RedisStreamsSourceConfig.DEFAULT_CONSUMER_GROUP);
        assertThat(config.getConsumerName()).startsWith("flink-consumer-");
        assertThat(config.isBounded()).isFalse();
        assertThat(config.getPollTimeout())
                .isEqualTo(RedisStreamsSourceConfig.DEFAULT_POLL_TIMEOUT_MS);
        assertThat(config.getBatchSize()).isEqualTo(RedisStreamsSourceConfig.DEFAULT_BATCH_SIZE);
        assertThat(config.getStartupMode()).isEqualTo(StartupMode.LATEST);
        assertThat(config.getMaxDeferredAckQueueSize())
                .isEqualTo(RedisStreamsSourceConfig.DEFAULT_MAX_DEFERRED_ACK_QUEUE_SIZE);
        assertThat(config.getCircuitBreakerFailureThreshold())
                .isEqualTo(RedisStreamsSourceConfig.DEFAULT_CIRCUIT_BREAKER_FAILURE_THRESHOLD);
        assertThat(config.getCircuitBreakerOpenDurationMs())
                .isEqualTo(RedisStreamsSourceConfig.DEFAULT_CIRCUIT_BREAKER_OPEN_DURATION_MS);
    }

    @Test
    void customConfigIsHonored() {
        RedisStreamsSourceConfig config =
                RedisStreamsSourceConfig.builder()
                        .setHost("redis.example.com")
                        .setPort(6380)
                        .setPassword("secret")
                        .setDatabase(3)
                        .setStreamKeys(Arrays.asList("a", "b"))
                        .setConsumerGroup("group")
                        .setConsumerName("worker")
                        .setBounded(true)
                        .setPollTimeout(500)
                        .setBatchSize(64)
                        .setStartupMode(StartupMode.EARLIEST)
                        .setMaxDeferredAckQueueSize(50)
                        .setCircuitBreakerFailureThreshold(2)
                        .setCircuitBreakerOpenDurationMs(1234L)
                        .build();

        assertThat(config.getHost()).isEqualTo("redis.example.com");
        assertThat(config.getPort()).isEqualTo(6380);
        assertThat(config.getPassword()).isEqualTo("secret");
        assertThat(config.getDatabase()).isEqualTo(3);
        assertThat(config.getStreamKeys()).containsExactly("a", "b");
        assertThat(config.getConsumerGroup()).isEqualTo("group");
        assertThat(config.getConsumerName()).isEqualTo("worker");
        assertThat(config.isBounded()).isTrue();
        assertThat(config.getPollTimeout()).isEqualTo(500);
        assertThat(config.getBatchSize()).isEqualTo(64);
        assertThat(config.getStartupMode()).isEqualTo(StartupMode.EARLIEST);
        assertThat(config.getMaxDeferredAckQueueSize()).isEqualTo(50);
        assertThat(config.getCircuitBreakerFailureThreshold()).isEqualTo(2);
        assertThat(config.getCircuitBreakerOpenDurationMs()).isEqualTo(1234L);
    }

    @Test
    void streamKeysListIsImmutable() {
        java.util.List<String> mutable = new java.util.ArrayList<>(List.of("a"));
        RedisStreamsSourceConfig config =
                RedisStreamsSourceConfig.builder().setStreamKeys(mutable).build();

        mutable.add("b"); // mutating the source must not affect the config
        assertThat(config.getStreamKeys()).containsExactly("a");

        assertThatThrownBy(() -> config.getStreamKeys().add("z"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void validationRejectsBadInputs() {
        // streamKeys missing
        assertThatThrownBy(() -> RedisStreamsSourceConfig.builder().build())
                .isInstanceOf(NullPointerException.class);
        // empty streamKeys
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setStreamKeys(Collections.emptyList())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        // null entry inside streamKeys
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setStreamKeys(Arrays.asList("a", null))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        // empty host
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setHost("")
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        // bad port
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setPort(0)
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        // negative database
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setDatabase(-1)
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        // empty consumerGroup
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setConsumerGroup("")
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        // empty consumerName
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setConsumerName("")
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        // non-positive sizes / thresholds
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setBatchSize(0)
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setMaxDeferredAckQueueSize(0)
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setCircuitBreakerFailureThreshold(0)
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceConfig.builder()
                                        .setCircuitBreakerOpenDurationMs(0L)
                                        .setStreamKeys(List.of("s"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
