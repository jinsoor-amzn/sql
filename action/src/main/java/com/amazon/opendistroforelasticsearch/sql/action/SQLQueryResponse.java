/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SQLQueryResponse extends ActionResponse {
    private String result;

    public SQLQueryResponse() {}

    public SQLQueryResponse(final String result) {
        this.result = result;
    }

    public static SQLQueryResponse fromActionResponse(final ActionResponse actionResponse) {
        try {
            if (actionResponse instanceof SQLQueryResponse) {
                return (SQLQueryResponse) actionResponse;
            }
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos);
            actionResponse.writeTo(osso);
            final InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
            final SQLQueryResponse response = new SQLQueryResponse();
            response.readFrom(input);
            return response;
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionResponse into SQLQueryResponse", e);
        }
    }

    public String getResult() {
        return result;
    }

    @Override public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        result = in.readString();
    }

    @Override public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(result);
    }
}
