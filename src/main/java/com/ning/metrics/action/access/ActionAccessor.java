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

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import com.ning.metrics.action.access.ActionCoreParser.ActionCoreParserFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;

public class ActionAccessor
{
    private static final Logger log = Logger.getLogger(ActionAccessor.class);
    private static final String ACTION_CORE_API_VERSION = "1.0";
    private final AsyncHttpClient client;
    private final String host;
    private final int port;
    private final String url;
    private final String DELIMITER = "|";

    public ActionAccessor(String host, int port)
    {
        this.host = host;
        this.port = port;
        this.url = String.format("http://%s:%d/rest/%s/json?path=", host, port, ACTION_CORE_API_VERSION);
        client = createHttpClient();
    }

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
        if (client != null) {
            client.close();
        }
    }

    /**
     * Synchronous interface: Returns a list of bean events.
     */
    public ImmutableList<Map<String, Object>> getPath(final String eventName, final String path,
                                                      final ActionCoreParserFormat format,
                                                      final ArrayList<String> desiredEventFields,
                                                      boolean recursive, boolean raw, long timeout)
    {
        InputStream in = null;
        try {
            Future<InputStream> future = getPath(eventName, path, recursive, raw);
            in = future.get(timeout, TimeUnit.SECONDS);
            String json = getJsonFromStreamAndClose(in);
            if (json == null) {
                return null;
            }
            ActionCoreParser parser = new ActionCoreParser(format, eventName, desiredEventFields, DELIMITER);
            return parser.parse(json);
        }
        catch (IOException ioe) {
            log.warn("IOException : Failed to connect to action code : url = " + url + ", error = " + ioe.getMessage());
            return null;
        }
        catch (InterruptedException ie) {
            log.warn("Thread got interrupted : Failed to connect to action code : url = " + url + ", error = " + ie.getMessage());
            return null;
        }
        catch (TimeoutException toe) {
            log.warn("Timeout: Failed to connect to action code within " + timeout + " sec, : url = " + url);
            return null;
        }
        catch (Throwable othere) {
            log.error("Unexpected exception while connecting to action core, url =  " + url, othere);
            return null;
        }
    }

    /**
     * Asynchronous interface: Returns a Future on which to wait.
     * <p/>
     * Client is responsible to close the stream
     */


    public Future<InputStream> getPath(final String eventName, final String path, boolean recursive, boolean raw)
    {
        try {
            String fullUrl = formatPath(path, recursive, raw);
            log.debug(String.format("ActionAccessor fetching %s", fullUrl));
            return client.prepareGet(fullUrl).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<InputStream>()
            {
                @Override
                public InputStream onCompleted(Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        log.warn(String.format("Failed to fetch path %s from %s got http status %d",
                            path, url, response.getStatusCode()));
                        return null;
                    }
                    return response.getResponseBodyAsStream();
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


    public String getJsonFromStreamAndClose(InputStream in)
    {
        if (in == null) {
            return null;
        }
        try {

            Reader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            int read = 0;
            char[] temp = new char[1024];
            final Writer writer = new StringWriter();
            while ((read = reader.read(temp)) != -1) {
                writer.write(temp, 0, read);
            }
            return writer.toString();
        }
        catch (IOException ioe) {
            log.warn("Failed to read from stream " + ioe.getMessage());
            return null;
        }
        finally {
            closeStream(in);
        }
    }


    private String formatPath(final String path, boolean recursive, boolean raw)
    {
        StringBuilder tmp = new StringBuilder();
        tmp.append(String.format("%s%s", url, path));
        String queryParam = "&";
        tmp.append(queryParam);
        tmp.append(recursive ? "recursive=true" : "recursive=false");
        tmp.append(queryParam);
        tmp.append(raw ? "raw=true" : "raw=false");
        return tmp.toString();
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