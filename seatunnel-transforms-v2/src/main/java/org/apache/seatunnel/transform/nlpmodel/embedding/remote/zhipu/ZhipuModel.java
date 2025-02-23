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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote.zhipu;

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import com.zhipu.oapi.ClientV4;
import com.zhipu.oapi.service.v4.embedding.Embedding;
import com.zhipu.oapi.service.v4.embedding.EmbeddingApiResponse;
import com.zhipu.oapi.service.v4.embedding.EmbeddingRequest;
import com.zhipu.oapi.service.v4.embedding.EmbeddingResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Zhipu model. Refer <a href="https://bigmodel.cn/dev/api/vector/embedding">embedding api </a> */
public class ZhipuModel extends AbstractModel {

    private final ClientV4 client;
    private final String model;
    private final Integer dimension;

    public ZhipuModel(String apiKey, String model, Integer dimension, Integer vectorizedNumber)
            throws IOException {
        super(vectorizedNumber);
        this.model = model;
        this.dimension = dimension;
        this.client =
                new ClientV4.Builder(apiKey)
                        .enableTokenCache()
                        .connectionPool(new okhttp3.ConnectionPool(8, 1, TimeUnit.SECONDS))
                        .build();
    }

    @Override
    public List<List<Double>> vector(Object[] fields) throws IOException {
        return vectorGeneration(fields);
    }

    @Override
    public Integer dimension() throws IOException {
        return dimension;
    }

    private List<List<Double>> vectorGeneration(Object[] fields) throws IOException {

        EmbeddingRequest embeddingRequest =
                EmbeddingRequest.builder()
                        .input(Arrays.asList(fields))
                        .dimensions(dimension)
                        .model(model)
                        .build();

        EmbeddingApiResponse response = client.invokeEmbeddingsApi(embeddingRequest);
        List<List<Double>> vector = new ArrayList<>();

        EmbeddingResult embeddingResult = response.getData();
        if (embeddingResult == null) {
            return vector;
        }
        List<Embedding> embeddings = embeddingResult.getData();
        embeddings.forEach(
                embedding -> {
                    vector.add(embedding.getEmbedding());
                });
        return vector;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
