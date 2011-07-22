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
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Request;
import com.ning.http.client.Response;
import com.ning.metrics.action.access.ActionCoreParser.ActionCoreParserFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
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

public class ActionAccessor
{
    private static final Logger log = LoggerFactory.getLogger(ActionAccessor.class);

    private static final String USER_AGENT = "action-access/1.0";
    // On our testing, we were doing 600 MB per minute on upload (80 megabits/second)
    private static final int CONNECTION_TIMEOUT_IN_MS = 5 * 60 * 1000; // 5 minutes

    private static final String ACTION_CORE_API_VERSION = "1.0";
    private final AsyncHttpClient client;
    private final String host;
    private final int port;
    private final String url;
    private final String DELIMITER = "|";

    public ActionAccessor(final String host, final int port)
    {
        this.host = host;
        this.port = port;
        this.url = String.format("http://%s:%d/rest/%s/json?path=", host, port, ACTION_CORE_API_VERSION);
        client = createHttpClient();
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
    public ImmutableList<Map<String, Object>> getPath(final String path,
                                                      final ActionCoreParserFormat format,
                                                      final ArrayList<String> desiredEventFields,
                                                      final boolean recursive,
                                                      final boolean raw,
                                                      final long timeout)
    {
        try {
            final Future<InputStream> future = getPath(path, recursive, raw);
            final InputStream in = future.get(timeout, TimeUnit.SECONDS);
            final String json = getJsonFromStreamAndClose(in);
            if (json == null) {
                return null;
            }
            final ActionCoreParser parser = new ActionCoreParser(format, desiredEventFields, DELIMITER);
            return parser.parse(json);
        }
        catch (IOException ioe) {
            log.warn("IOException: Failed to connect to action code: url = {}, error = {}", url, ioe.getMessage());
            return null;
        }
        catch (InterruptedException ie) {
            log.warn("Thread got interrupted: Failed to connect to action code: url = {}, error =  {}", url, ie.getMessage());
            Thread.currentThread().interrupt();
            return null;
        }
        catch (TimeoutException toe) {
            log.warn("Timeout: Failed to connect to action code within {} sec, url = {}", timeout, url);
            return null;
        }
        catch (Throwable other) {
            log.error("Unexpected exception while connecting to action core, url = {}, error = {}", url, other.getMessage());
            return null;
        }
    }

    /**
     * Asynchronous interface: Returns a Future on which to wait.
     * <p/>
     * Client is responsible to close the stream
     */
    public Future<InputStream> getPath(final String path, final boolean recursive, final boolean raw)
    {
        try {
            final String fullUrl = formatPath(path, recursive, raw);
            log.debug("ActionAccessor fetching {}", fullUrl);
            return client.prepareGet(fullUrl).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<InputStream>()
            {
                @Override
                public InputStream onCompleted(final Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        log.warn("Failed to fetch path {} from {} got http status {}",
                            new Object[]{path, url, response.getStatusCode()});
                        return null;
                    }
                    return response.getResponseBodyAsStream();
                }

                @Override
                public void onThrowable(Throwable t)
                {
                    log.warn("Failed to contact action-core", t);
                }
            });
        }
        catch (IOException e) {
            log.warn("Error getting path {} from {}:{} ({})", new Object[]{path, host, port, e.getLocalizedMessage()});
            return null;
        }
    }

    /**
     * Asynchronous interface to upload files to HDFS
     *
     * @param file       local file to upload
     * @param outputPath full path on HDFS
     * @return a Future on the action-core Response
     * @throws IOException generic I/O Exception
     */
    public ListenableFuture<Response> upload(final File file, final String outputPath) throws IOException
    {
        return upload(file, outputPath, false, (short) 3, -1, "u=rw,go=r");
    }

    /**
     * Asynchronous interface to upload files to HDFS
     *
     * @param file        local file to upload
     * @param outputPath  full path on HDFS
     * @param overwrite   whether an existing file should be overwritten on HDFS
     * @param replication replication factor of the file
     * @param blocksize   blocksize for I/O
     * @param permission  file's permissions
     * @return a Future on the action-core response
     * @throws IOException generic I/O Exception
     */
    public ListenableFuture<Response> upload(
        final File file,
        final String outputPath,
        final boolean overwrite,
        final short replication,
        final long blocksize,
        final String permission
    ) throws IOException
    {
        final Request request = client.preparePost(String.format("http://%s:%d/rest/%s", host, port, ACTION_CORE_API_VERSION))
            .setBody(file)
            .addQueryParameter("path", outputPath)
            .addQueryParameter("overwrite", String.valueOf(overwrite))
            .addQueryParameter("replication", String.valueOf(replication))
            .addQueryParameter("blocksize", String.valueOf(blocksize))
            .addQueryParameter("permission", permission)
            .build();
        log.info("Sending local file to HDFS: {}", file.getAbsolutePath());
        return client.executeRequest(request);
    }

    private String formatPath(final String path, final boolean recursive, final boolean raw)
    {
        final StringBuilder tmp = new StringBuilder();
        tmp.append(String.format("%s%s", url, path));
        final String queryParam = "&";
        tmp.append(queryParam);
        tmp.append(recursive ? "recursive=true" : "recursive=false");
        tmp.append(queryParam);
        tmp.append(raw ? "raw=true" : "raw=false");
        return tmp.toString();
    }

    private String getJsonFromStreamAndClose(final InputStream in)
    {
        if (in == null) {
            return null;
        }
        try {

            final Reader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            int read;
            final char[] temp = new char[1024];
            final Writer writer = new StringWriter();
            while ((read = reader.read(temp)) != -1) {
                writer.write(temp, 0, read);
            }
            return writer.toString();
        }
        catch (IOException ioe) {
            log.warn("Failed to read from stream {}", ioe.getMessage());
            return null;
        }
        finally {
            closeStream(in);
        }
    }

    private void closeStream(final InputStream in)
    {
        if (in != null) {
            try {
                in.close();
            }
            catch (IOException e) {
                log.warn("Failed to close http-client - provided InputStream: {}", e.getLocalizedMessage());
            }
        }
    }

    private static AsyncHttpClient createHttpClient()
    {
        // Don't limit the number of connections per host
        // See https://github.com/ning/async-http-client/issues/issue/28
        final AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder()
            .setMaximumConnectionsPerHost(-1)
            .setUserAgent(USER_AGENT)
            .setConnectionTimeoutInMs(CONNECTION_TIMEOUT_IN_MS)
            .setRequestTimeoutInMs(CONNECTION_TIMEOUT_IN_MS);
        return new AsyncHttpClient(builder.build());
    }
}
