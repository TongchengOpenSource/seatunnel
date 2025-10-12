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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink.commiter;

import org.apache.seatunnel.api.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for StarRocks commit info */
public class StarRocksCommitInfoSerializer implements Serializer<StarRocksCommitInfo> {

    @Override
    public byte[] serialize(StarRocksCommitInfo commitInfo) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeUTF(commitInfo.getHostPort());
            dos.writeUTF(commitInfo.getDb());
            dos.writeUTF(commitInfo.getLabel());

            return baos.toByteArray();
        }
    }

    @Override
    public StarRocksCommitInfo deserialize(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream dis = new DataInputStream(bais)) {
            String hostPort = dis.readUTF();
            String db = dis.readUTF();
            String label = dis.readUTF();
            return new StarRocksCommitInfo(hostPort, label, db);
        }
    }
}
