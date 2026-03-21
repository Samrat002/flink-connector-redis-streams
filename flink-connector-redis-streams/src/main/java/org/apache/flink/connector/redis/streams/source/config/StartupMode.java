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

/**
 * Startup mode for the Redis Streams source when a consumer group does not yet exist.
 *
 * <p>Once the consumer group exists (BUSYGROUP), Redis tracks position server-side and this setting
 * has no effect — the connector resumes from wherever the group left off.
 */
@PublicEvolving
public enum StartupMode {

    /**
     * Start from the beginning of the stream (Redis offset {@code 0-0}).
     *
     * <p>Use when you need to process the full stream history on first deployment.
     */
    EARLIEST,

    /**
     * Start from the latest entry at the time the consumer group is created (Redis offset {@code
     * $}).
     *
     * <p>This is the default. Use when you only care about new messages and do not need to replay
     * history.
     */
    LATEST
}
