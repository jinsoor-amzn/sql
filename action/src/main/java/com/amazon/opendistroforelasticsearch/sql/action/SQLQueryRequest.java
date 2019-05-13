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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SQLQueryRequest extends ActionRequest {

    private String format;
    private String queryString;

    public SQLQueryRequest() {}

    public SQLQueryRequest(final String format, final String queryString) {
        this.format = format;
        this.queryString = queryString;
    }

    public static SQLQueryRequest fromActionRequest(final ActionRequest actionRequest) {
        try {
            if (actionRequest instanceof SQLQueryRequest) {
                return (SQLQueryRequest) actionRequest;
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos);
            actionRequest.writeTo(osso);
            final StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
            final SQLQueryRequest request = new SQLQueryRequest();
            request.readFrom(input);
            return request;
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into SQLQueryRequest", e);
        }
    }

    public String getQueryString() {
        return queryString;
    }

    public String getFormat() {
        return format;
    }

    @Override public ActionRequestValidationException validate() {
        return null;
    }

    @Override public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        format = in.readString();
        queryString = in.readString();
    }

    @Override public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(format);
        out.writeString(queryString);
    }
}
