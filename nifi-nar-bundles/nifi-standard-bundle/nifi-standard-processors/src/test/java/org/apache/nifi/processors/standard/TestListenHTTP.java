/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;

import static org.apache.nifi.processors.standard.ListenHTTP.RELATIONSHIP_SUCCESS;
import static org.junit.Assert.fail;

public class TestListenHTTP {

    private static final String SSL_CONTEXT_SERVICE_IDENTIFIER = "ssl-context";

    private static final String HTTP_POST_METHOD = "POST";
    private static final String HTTP_BASE_PATH = "basePath";

    private final static String PORT_VARIABLE = "HTTP_PORT";
    private final static String HTTP_SERVER_PORT_EL = "${" + PORT_VARIABLE + "}";

    private final static String BASEPATH_VARIABLE = "HTTP_BASEPATH";
    private final static String HTTP_SERVER_BASEPATH_EL = "${" + BASEPATH_VARIABLE + "}";

    private ListenHTTP proc;
    private TestRunner runner;

    private int availablePort;

    @Before
    public void setup() throws IOException {
        proc = new ListenHTTP();
        runner = TestRunners.newTestRunner(proc);
        availablePort = NetworkUtils.availablePort();;
        runner.setVariable(PORT_VARIABLE, Integer.toString(availablePort));
        runner.setVariable(BASEPATH_VARIABLE, HTTP_BASE_PATH);

    }

    @After
    public void teardown() {
        proc.shutdownHttpServer();
    }

    @Test
    public void testPOSTRequestsReceivedWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, false, false);
    }

    @Test
    public void testPOSTRequestsReceivedReturnCodeWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, false, false);
    }

    @Test
    public void testPOSTRequestsReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, false, false);
    }

    @Test
    public void testPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, false, false);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureInvalidSSLConfiguration() throws Exception {
        SSLContextService sslContextService = configureInvalidProcessorSslContextService();
        runner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertNotValid();
    }

    private int executePOST(String message, boolean secure, boolean twoWaySsl) throws Exception {
        final String scheme = secure ? "https" : "http";
        final URL url = new URL(scheme + "://localhost:" + availablePort + "/" + HTTP_BASE_PATH);
        HttpURLConnection connection;

        if (secure) {
            final HttpsURLConnection sslCon = (HttpsURLConnection) url.openConnection();
            if (twoWaySsl) {
                // use a client certificate, do not reuse the server's keystore
                SSLContext clientSslContext = SslContextFactory.createSslContext(
                        "src/test/resources/client-keystore.p12",
                        "passwordpassword".toCharArray(),
                        "PKCS12",
                        "src/test/resources/truststore.jks",
                        "passwordpassword".toCharArray(),
                        "JKS",
                        null,
                        "TLSv1.2");
                sslCon.setSSLSocketFactory(clientSslContext.getSocketFactory());
            } else {
                // with one-way SSL, the client still needs a truststore
                SSLContext clientSslContext = SslContextFactory.createTrustSslContext(
                        "src/test/resources/truststore.jks",
                        "passwordpassword".toCharArray(),
                        "JKS",
                        "TLSv1.2");
                sslCon.setSSLSocketFactory(clientSslContext.getSocketFactory());
            }
            connection = sslCon;
        } else {
            connection = (HttpURLConnection) url.openConnection();
        }
        connection.setRequestMethod(HTTP_POST_METHOD);
        connection.setDoOutput(true);

        final DataOutputStream wr = new DataOutputStream(connection.getOutputStream());

        if (message != null) {
            wr.writeBytes(message);
        }
        wr.flush();
        wr.close();
        return connection.getResponseCode();
    }

    private void testPOSTRequestsReceived(int returnCode, boolean secure, boolean twoWaySsl) throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");
        messages.add("");
        messages.add(null);
        messages.add("payload 2");

        startWebServerAndSendMessages(messages, returnCode, secure, twoWaySsl);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS);

        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 4);
        mockFlowFiles.get(0).assertContentEquals("payload 1");
        mockFlowFiles.get(1).assertContentEquals("");
        mockFlowFiles.get(2).assertContentEquals("");
        mockFlowFiles.get(3).assertContentEquals("payload 2");
    }

    private void startWebServerAndSendMessages(final List<String> messages, int returnCode, boolean secure, boolean twoWaySsl)
            throws Exception {

        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.createHttpServer(context);

        Runnable sendMessagestoWebServer = () -> {
            try {
                for (final String message : messages) {
                    if (executePOST(message, secure, twoWaySsl) != returnCode) {
                        fail("HTTP POST failed.");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail("Not expecting error here.");
            }
        };
        new Thread(sendMessagestoWebServer).start();

        long responseTimeout = 10000;

        int numTransferred = 0;
        long startTime = System.currentTimeMillis();
        while (numTransferred < messages.size() && (System.currentTimeMillis() - startTime < responseTimeout)) {
            proc.onTrigger(context, processSessionFactory);
            numTransferred = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS).size();
            Thread.sleep(100);
        }

        runner.assertTransferCount(ListenHTTP.RELATIONSHIP_SUCCESS, messages.size());

    }

    private SSLContextService configureProcessorSslContextService(boolean twoWaySsl) throws InitializationException {
        final SSLContextService sslContextService = new StandardRestrictedSSLContextService();
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextService);
        if (twoWaySsl) {
            runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
            runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
            runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        }
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");

        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);
        return sslContextService;
    }

    private SSLContextService configureInvalidProcessorSslContextService() throws InitializationException {
        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");

        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);
        return sslContextService;
    }
}
