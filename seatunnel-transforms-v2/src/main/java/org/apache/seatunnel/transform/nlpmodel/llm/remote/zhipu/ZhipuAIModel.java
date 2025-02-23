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

package org.apache.seatunnel.transform.nlpmodel.llm.remote.zhipu;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.AbstractModel;

import com.zhipu.oapi.ClientV4;
import com.zhipu.oapi.service.v4.model.ChatCompletionRequest;
import com.zhipu.oapi.service.v4.model.ChatMessage;
import com.zhipu.oapi.service.v4.model.ChatMessageRole;
import com.zhipu.oapi.service.v4.model.Choice;
import com.zhipu.oapi.service.v4.model.ModelApiResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Zhipu model. Refer <a href="https://bigmodel.cn/dev/api/normal-model/glm-4">glm-4 api </a> */
public class ZhipuAIModel extends AbstractModel {

    private final ClientV4 client;
    private final String model;

    public ZhipuAIModel(
            SeaTunnelRowType rowType,
            SqlType outputType,
            List<String> projectionColumns,
            String prompt,
            String model,
            String apiKey) {
        super(rowType, outputType, projectionColumns, prompt);
        this.model = model;
        this.client =
                new ClientV4.Builder(apiKey)
                        .enableTokenCache()
                        .connectionPool(new okhttp3.ConnectionPool(8, 1, TimeUnit.SECONDS))
                        .build();
    }

    @Override
    protected List<String> chatWithModel(String promptWithLimit, String rowsJson)
            throws IOException {

        List<ChatMessage> messages = new ArrayList<>();
        ChatMessage systemMessage =
                new ChatMessage(ChatMessageRole.SYSTEM.value(), promptWithLimit);
        ChatMessage userMessage = new ChatMessage(ChatMessageRole.USER.value(), rowsJson);
        messages.add(systemMessage);
        messages.add(userMessage);

        ChatCompletionRequest chatCompletionRequest =
                ChatCompletionRequest.builder().model(model).messages(messages).build();

        ModelApiResponse chatResponse = client.invokeModelApi(chatCompletionRequest);
        if (!chatResponse.isSuccess()) {
            throw new IOException("Failed to chat with model, response: " + chatResponse.getMsg());
        }
        List<String> results = new ArrayList<>();
        for (Choice choice : chatResponse.getData().getChoices()) {
            results.add(choice.getMessage().getContent().toString());
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
