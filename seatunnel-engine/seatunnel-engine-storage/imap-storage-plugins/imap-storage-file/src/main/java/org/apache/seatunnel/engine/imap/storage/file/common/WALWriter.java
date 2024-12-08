/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.api.common.ImapStorageThreadFactory;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.wal.DiscoveryWalFileFactory;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.IFileWriter;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WALWriter implements AutoCloseable {

    IFileWriter writer;

    private ExecutorService walWriterService;

    Future<?> writeTaskFuture;

    private static final int DEFAULT_THREAD_POOL_MIN_SIZE =
            Runtime.getRuntime().availableProcessors() * 2 + 1;

    private static final int DEFAULT_THREAD_POOL_MAX_SIZE =
            Runtime.getRuntime().availableProcessors() * 4 + 1;

    private static final int DEFAULT_THREAD_POOL_QUENE_SIZE = 1024;

    private final WALSyncType walSyncType;

    public WALWriter(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            WALSyncType walSyncType,
            Path parentPath,
            Serializer serializer)
            throws IOException {
        this.writer = DiscoveryWalFileFactory.getWriter(fileConfiguration.getName());
        this.writer.setBlockSize(fileConfiguration.getConfiguration().getBlockSize());
        this.writer.initialize(fs, parentPath, serializer);
        this.walSyncType = walSyncType;
        if (WALSyncType.ASYNC == walSyncType) {
            this.walWriterService =
                    new ThreadPoolExecutor(
                            DEFAULT_THREAD_POOL_MIN_SIZE,
                            DEFAULT_THREAD_POOL_MAX_SIZE,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(DEFAULT_THREAD_POOL_QUENE_SIZE),
                            new ImapStorageThreadFactory());
        }
    }

    public void write(IMapFileData data) throws IOException {

        switch (walSyncType) {
            case SYNC:
                this.writer.write(data);
                return;
            case ASYNC:
                writeTaskFuture =
                        this.walWriterService.submit(
                                () -> {
                                    try {
                                        this.writer.write(data);
                                    } catch (Exception e) {
                                        log.error(String.format("store imap failed : %s", data), e);
                                    }
                                });
                return;
        }
    }

    @Override
    public void close() throws Exception {
        if (WALSyncType.ASYNC == walSyncType && writeTaskFuture != null) {
            writeTaskFuture.cancel(false);
            if (walWriterService != null) {
                walWriterService.shutdown();
            }
        }
        this.writer.close();
    }
}
