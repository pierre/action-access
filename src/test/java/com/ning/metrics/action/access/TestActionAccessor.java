package com.ning.metrics.action.access;

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

import javax.servlet.ServletException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

public class TestActionAccessor
{
    private static final String SRC_TEST_RESOURCES = "src/test/resources";
    private static final String ACTION_CORE_BASE_PATH = "/rest/1.0/json?path=/events/qa/smileEvent";

    private int port;
    private SocketConnector connector;
    private Server server;
    private final AtomicInteger serversHits = new AtomicInteger(0);
    private boolean jettyShouldBomb = false;

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
                if (request.getPathInfo().equals(ACTION_CORE_BASE_PATH + "/2011/05")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.json");
                }
                else if (request.getPathInfo().equals(ACTION_CORE_BASE_PATH + "/2011/05&recursive=true&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.recursive.json");
                }
                else if (request.getPathInfo().equals(ACTION_CORE_BASE_PATH + "/2011/05/03")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.03.json");
                }
                else if (request.getPathInfo().equals(ACTION_CORE_BASE_PATH + "/2011/05/03&recursive=true&raw=false")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.03.recursive.json");
                }
                else if (request.getPathInfo().equals(ACTION_CORE_BASE_PATH + "/2011/05/03/21")) {
                    file = new File(SRC_TEST_RESOURCES + "/events.qa.smileEvent.2011.05.03.21.json");
                }
                else if (request.getPathInfo().equals(ACTION_CORE_BASE_PATH + "/2011/05/03/21&recursive=true&raw=false")) {
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

    @Test(groups = "slow")
    public void testAccess() throws Exception
    {
        ActionAccessor accessor = new ActionAccessor("127.0.0.1", port);
        Assert.assertEquals(serversHits.get(), 0);

        accessor.getFile("/events/qa/smileEvent/2011/05", false).get();
        Assert.assertEquals(serversHits.get(), 1);

        accessor.close();
    }
}
