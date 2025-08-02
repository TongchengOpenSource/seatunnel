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

package org.apache.seatunnel.connectors.seatunnel.fluss.config;

/**
 * Sink-specific options for Fluss connector, following the official Fluss design pattern.
 *
 * <p>This class only defines sink-specific options that are not covered by the core Fluss
 * configuration. Performance, transaction, and client-related options should be configured through
 * the fluss.config map.
 *
 * <p>All sink options are already defined in {@link FlussOptions} as they are shared between source
 * and sink connectors:
 *
 * <ul>
 *   <li>{@link FlussOptions#SINK_IGNORE_DELETE} - Whether to ignore delete records
 *   <li>{@link FlussOptions#SINK_BUCKET_SHUFFLE} - Whether to shuffle by bucket id
 * </ul>
 *
 * <p>Other sink configurations should be specified in fluss.config:
 *
 * <pre>{@code
 * fluss.config = {
 *   "batch.size" = "1000"
 *   "batch.timeout.ms" = "5000"
 *   "enable.transaction" = "true"
 *   "transaction.timeout" = "15min"
 *   "enable.exactly.once" = "false"
 *   "transactional.id.prefix" = "seatunnel-fluss"
 * }
 * }</pre>
 */
public class FlussSinkOptions extends FlussOptions {

    // All sink-specific options are defined in FlussOptions
    // This class exists for consistency and future sink-specific extensions
}
