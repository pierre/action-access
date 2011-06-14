/*
 * Copyright 2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.action.access;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Future;

public class ActionAccessor
{
    private static final Logger log = Logger.getLogger(ActionAccessor.class);
    private static final short ACTION_CORE_API_VERSION = 1;

    private static final ObjectMapper mapper = new ObjectMapper();

    private final AsyncHttpClient client;

    private final String host;
    private final int port;
    private String url;


    public ActionAccessor(String host, int port)
    {
        this.host = host;
        this.port = port;

        this.url = String.format("http://%s:%d/rest/%d", host, port, ACTION_CORE_API_VERSION);

        client = createHttpClient();
    }

    // note: if called from base-class constructor, couldn't sub-class; hence just make static
    private static AsyncHttpClient createHttpClient()
    {
        // Don't limit the number of connections per host
        // See https://github.com/ning/async-http-client/issues/issue/28
        AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setMaximumConnectionsPerHost(-1);
        return new AsyncHttpClient(builder.build());
    }

    /**
     * Close the underlying http client
     */
    public synchronized void close()
    {
        client.close();
    }

    public Future<JsonNode> getFile(String path, boolean recursive)
    {
        try {
            return client.prepareGet(String.format("%s/%s", url, path)).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<JsonNode>()
            {
                @Override
                public JsonNode onCompleted(Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        return null;
                    }

                    InputStream in = response.getResponseBodyAsStream();
                    try {
                        return mapper.readValue(in, JsonNode.class);
                    }
                    finally {
                        closeStream(in);
                    }
                }

                @Override
                public void onThrowable(Throwable t)
                {
                    log.warn(t);
                }
            });
        }
        catch (IOException e) {
            log.warn(String.format("Error getting path %s from %s:%d (%s)", path, host, port, e.getLocalizedMessage()));
            return null;
        }
    }

    private void closeStream(InputStream in)
    {
        if (in != null) {
            try {
                in.close();
            }
            catch (IOException e) {
                log.warn(String.format("Failed to close http-client - provided InputStream: %s", e.getLocalizedMessage()));
            }
        }
    }
}
