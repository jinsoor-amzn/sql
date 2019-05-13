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

package com.amazon.opendistroforelasticsearch.sql.plugin;

import com.amazon.opendistroforelasticsearch.sql.action.SQLAction;
import com.amazon.opendistroforelasticsearch.sql.action.SQLQueryRequest;
import com.amazon.opendistroforelasticsearch.sql.action.SQLQueryResponse;
import com.amazon.opendistroforelasticsearch.sql.exception.SqlParseException;
import com.amazon.opendistroforelasticsearch.sql.executor.ActionRequestRestExecutorFactory;
import com.amazon.opendistroforelasticsearch.sql.executor.RestExecutor;
import com.amazon.opendistroforelasticsearch.sql.query.QueryAction;
import com.amazon.opendistroforelasticsearch.sql.request.SqlRequest;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.json.JSONObject;

import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;

public class SQLTransportAction extends HandledTransportAction<ActionRequest, SQLQueryResponse> {

    private Client client;

    @Inject
    public SQLTransportAction(final Settings settings, final ThreadPool threadPool, final TransportService transportService, final ActionFilters actionFilters, final IndexNameExpressionResolver indexNameExpressionResolver, final Client client) {
        super(settings, SQLAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SQLQueryRequest::new);
        this.client = client;
    }

    @Override protected void doExecute(final ActionRequest actionRequest, final ActionListener<SQLQueryResponse> listener) {
        try {
            final SQLQueryRequest request = SQLQueryRequest.fromActionRequest(actionRequest);

            final JSONObject jsonObject = new JSONObject();
            jsonObject.append("query", request.getQueryString());
            final SqlRequest sqlRequest = new SqlRequest(request.getQueryString(), jsonObject);
            final QueryAction queryAction = new SearchDao(client).explain(sqlRequest.getSql());
            queryAction.setSqlRequest(sqlRequest);

            RestExecutor restExecutor = ActionRequestRestExecutorFactory.createExecutor(request.getFormat(), queryAction);

            final String result = restExecutor.execute(client, Collections.EMPTY_MAP, queryAction);
            listener.onResponse(new SQLQueryResponse(result));
        } catch (SQLFeatureNotSupportedException e) {
            listener.onFailure(e);
        } catch (SqlParseException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
