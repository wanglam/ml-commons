/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.transport.model;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.core.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelState;

import java.io.IOException;

import static org.junit.Assert.*;

public class MLModelGetResponseTest {

    MLModel mlModel;

    @Before
    public void setUp() {
        mlModel = MLModel.builder()
                .name("model")
                .algorithm(FunctionName.KMEANS)
                .version("1.0.0")
                .content("content")
                .user(new User())
                .modelState(MLModelState.TRAINED)
                .build();
    }

    @Test
    public void writeTo_Success() throws IOException {
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        MLModelGetResponse response = MLModelGetResponse.builder().mlModel(mlModel).build();
        response.writeTo(bytesStreamOutput);
        MLModelGetResponse parsedResponse = new MLModelGetResponse(bytesStreamOutput.bytes().streamInput());
        assertNotEquals(response.mlModel, parsedResponse.mlModel);
        assertEquals(response.mlModel.getName(), parsedResponse.mlModel.getName());
        assertEquals(response.mlModel.getAlgorithm(), parsedResponse.mlModel.getAlgorithm());
        assertEquals(response.mlModel.getVersion(), parsedResponse.mlModel.getVersion());
        assertEquals(response.mlModel.getContent(), parsedResponse.mlModel.getContent());
        assertEquals(response.mlModel.getUser(), parsedResponse.mlModel.getUser());
    }

    @Test
    public void toXContentTest() throws IOException {
        MLModelGetResponse mlModelGetResponse = MLModelGetResponse.builder().mlModel(mlModel).build();
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        mlModelGetResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertNotNull(builder);
        String jsonStr = org.opensearch.common.Strings.toString(builder);
        assertEquals("{\"name\":\"model\"," +
                "\"algorithm\":\"KMEANS\"," +
                "\"model_version\":\"1.0.0\"," +
                "\"model_content\":\"content\"," +
                "\"user\":{\"name\":\"\",\"backend_roles\":[],\"roles\":[],\"custom_attribute_names\":[],\"user_requested_tenant\":null},\"model_state\":\"TRAINED\"}",
                jsonStr);
    }
}
