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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;

public class SQLAction extends Action<SQLQueryRequest, ActionResponse, SQLQueryRequestBuilder> {

    public static final String NAME = "cluster:admin/_opendistro/_sql";
    public static final SQLAction INSTANCE = new SQLAction(NAME);

    protected SQLAction(final String name) {
        super(name);
    }

    @Override public SQLQueryRequestBuilder newRequestBuilder(final ElasticsearchClient client) {
        return null;
    }

    @Override public SQLQueryResponse newResponse() {
        return null;
    }
}
