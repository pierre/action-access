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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;

import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.inject.internal.ImmutableList;
import com.ning.metrics.action.access.ActionCoreParser.ActionCoreParserFormat;

public class TestActionAccessor
{
    private static final String SRC_TEST_RESOURCES = "src/test/resources";
    private static final String ACTION_CORE_BASE_PATH_PREFIX = "/events/qa/smileEvent";
    private static final String ACTION_CORE_BASE_PATH = "/rest/1.0/json?path=" + ACTION_CORE_BASE_PATH_PREFIX;

    private int port;
    private SocketConnector connector;
    private Server server;
    private final AtomicInteger serversHits = new AtomicInteger(0);
    private final boolean jettyShouldBomb = false;

    @BeforeClass(alwaysRun = true)
    public void setUpGlobal() throws Exception
    {
        port = findFreePort();
        connector = new SocketConnector();
        connector.setHost("127.0.0.1");
        connector.setPort(port);

        server = new Server()
        {
            @Override
            public void handle(HttpConnection connection) throws IOException, ServletException
            {
                serversHits.incrementAndGet();
                final Request request = connection.getRequest();
                final Response response = connection.getResponse();

                if (jettyShouldBomb) {
                    response.setStatus(500);
                    request.setHandled(true);
                    return;
                }
                else {
                    response.setStatus(200);
                }

                File file = null;

                String completePath = request.getUri().getCompletePath();
                completePath =  URLDecoder.decode(completePath, "UTF-8");


                if (completePath.equals(ACTION_CORE_BASE_PATH + "/timeSeries&recursive=true&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/timeSeries.json");
                }
                else if (completePath.equals(ACTION_CORE_BASE_PATH + "/2011/05&recursive=false&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.json");
                }
                else if (completePath.equals(ACTION_CORE_BASE_PATH + "/2011/05&recursive=true&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.recursive.json");
                }
                else if (completePath.equals(ACTION_CORE_BASE_PATH + "/2011/05/03&recursive=false&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.03.json");
                }
                else if (completePath.equals(ACTION_CORE_BASE_PATH + "/2011/05/03&recursive=true&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.03.recursive.json");
                }
                else if (completePath.equals(ACTION_CORE_BASE_PATH + "/2011/05/03/21&recursive=false&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.03.21.json");
                }
                else if (completePath.equals(ACTION_CORE_BASE_PATH + "/2011/05/03/21&recursive=true&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.03.21.recursive.json");
                }

                if (file != null) {
                    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    final InputStream is = new FileInputStream(file);
                    byte[] temp = new byte[1024];
                    int read;

                    while ((read = is.read(temp)) > 0) {
                        buffer.write(temp, 0, read);
                    }

                    byte[] data = buffer.toByteArray();
                    response.getWriter().print(new String(data));
                }
                request.setHandled(true);
            }
        };
        server.setThreadPool(new ExecutorThreadPool());
        server.addConnector(connector);
        connector.start();
        server.start();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownGlobal() throws Exception
    {
        server.stop();
        connector.stop();
    }

    private int findFreePort() throws IOException
    {
        ServerSocket socket = null;

        try {
            socket = new ServerSocket(0);

            return socket.getLocalPort();
        }
        finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    //
    // Test all combinations for synchronous operations. Since Synchronous API relies of async one, that should be good enough.
    //


    @Test(groups = "slow", enabled=true)
    public void testAccessRecursiveTimeSeries() throws Exception
    {
        testAccessSync(ActionCoreParserFormat.ACTION_CORE_FORMAT_MR, true, "/timeSeries", 35);
    }


    @Test(groups = "slow", enabled=true)
    public void testAccessRecursiveSync1() throws Exception
    {
        testAccessSync(ActionCoreParserFormat.ACTION_CORE_FORMAT_DEFAULT, true, "/2011/05/03/21", 1);
    }

    @Test(groups = "slow", enabled=true)
    public void testAccessRecursiveSync2() throws Exception
    {
        testAccessSync(ActionCoreParserFormat.ACTION_CORE_FORMAT_DEFAULT, true, "/2011/05/03", 2);
    }

    @Test(groups = "slow", enabled=true)
    public void testAccessRecursiveSync3() throws Exception
    {
        testAccessSync(ActionCoreParserFormat.ACTION_CORE_FORMAT_DEFAULT, true, "/2011/05", 2);
    }


    @Test(groups = "slow", enabled=true)
    public void testAccessSync1() throws Exception
    {
        testAccessSync(ActionCoreParserFormat.ACTION_CORE_FORMAT_DEFAULT, false, "/2011/05/03/21", 1);
    }

    @Test(groups = "slow", enabled=true)
    public void testAccessSync2() throws Exception
    {
        testAccessSync(ActionCoreParserFormat.ACTION_CORE_FORMAT_DEFAULT, false, "/2011/05/03", 0);
    }

    @Test(groups = "slow", enabled=true)
    public void testAccessSync3() throws Exception
    {
        testAccessSync(ActionCoreParserFormat.ACTION_CORE_FORMAT_DEFAULT, false, "/2011/05", 0);
    }

    private void testAccessSync(ActionCoreParserFormat format, boolean recursive, String pathDate, int expectedEventSize) throws Exception
    {
        ActionAccessor accessor = new ActionAccessor("127.0.0.1", port);
        String [] desiredEventsTimeSeries = {"ts", "duration", "ipSrc", "ipDst"};
        String [] desiredEventsOther =  {"1", "2", "3"};

        final String [] desiredEvents = (format == ActionCoreParserFormat.ACTION_CORE_FORMAT_MR) ? desiredEventsTimeSeries : desiredEventsOther;

        ImmutableList<Map<String, Object>> events = accessor.getPath("my-event",  ACTION_CORE_BASE_PATH_PREFIX + pathDate, format, desiredEvents, recursive, false, 5);
        Assert.assertTrue(events.size() == expectedEventSize, "expected " + expectedEventSize + " for path " + ", got " +  (events == null ? 0 : events.size()));
        accessor.close();
    }
}
