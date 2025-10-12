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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StreamLoadResponse;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.commiter.StarRocksCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.state.StarRocksSinkState;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface SinkManager {

    void init();

    void write(String record) throws IOException;

    void callback(StreamLoadResponse response);

    void callback(Throwable e);

    void flush() throws IOException;

    List<StarRocksSinkState> snapshot(long checkpointId);

    void beginTransaction(long checkpointId);

    Optional<StarRocksCommitInfo> prepareCommit();

    boolean abort(long checkpointId, int subTaskIndex) throws Exception;

    boolean abort() throws Exception;

    boolean commit(String transactionId);

    void close() throws Exception;
}
