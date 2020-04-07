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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import okhttp3.Call;
import okhttp3.Callback;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestHandleHttpRequest {

    private HandleHttpRequest processor;

    private static Map<String, String> getTruststoreProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/localhost-ts.jks");
        props.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "localtest");
        props.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        return props;
    }

    private static Map<String, String> getServerKeystoreProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/localhost-ks.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "localtest");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        return properties;
    }

    private static Map<String, String> getClientKeystoreProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/client-keystore.p12");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "passwordpassword");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
        return properties;
    }

    private static SSLContext useSSLContextService(final TestRunner controller, final Map<String, String> sslProperties, SSLContextService.ClientAuth clientAuth) {
        final SSLContextService service = new StandardRestrictedSSLContextService();
        try {
            controller.addControllerService("ssl-service", service, sslProperties);
            controller.enableControllerService(service);
        } catch (InitializationException ex) {
            ex.printStackTrace();
            Assert.fail("Could not create SSL Context Service");
        }

        controller.setProperty(HandleHttpRequest.SSL_CONTEXT, "ssl-service");
        return service.createSSLContext(clientAuth);
    }

    @After
    public void tearDown() throws Exception {
        if (processor != null) {
            processor.shutdown();
        }
    }

    @Test(timeout=30000)
    public void testRequestAddedToService() throws InitializationException, MalformedURLException, IOException, InterruptedException {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();
                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                    final HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:"
                            + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();

                    connection.setDoOutput(false);
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("header1", "value1");
                    connection.setRequestProperty("header2", "");
                    connection.setRequestProperty("header3", "apple=orange");
                    connection.setConnectTimeout(30000);
                    connection.setReadTimeout(30000);

                    sendRequest(connection, requestSent);
                } catch (final Throwable t) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        httpThread.start();
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 1);
        assertEquals(1, contextMap.size());

        final MockFlowFile mff = runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("http.query.param.query", "true");
        mff.assertAttributeEquals("http.query.param.value1", "value1");
        mff.assertAttributeEquals("http.query.param.value2", "");
        mff.assertAttributeEquals("http.query.param.value3", "");
        mff.assertAttributeEquals("http.query.param.value4", "apple=orange");
        mff.assertAttributeEquals("http.headers.header1", "value1");
        mff.assertAttributeEquals("http.headers.header3", "apple=orange");
    }

    @Test(timeout=10000)
    public void testFailToRegister() throws InitializationException, MalformedURLException, IOException, InterruptedException {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);
        CountDownLatch resultReady = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");
        contextMap.setRegisterSuccessfully(false);

        final int[] responseCode = new int[1];
        responseCode[0] = 0;
        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                HttpURLConnection connection = null;
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                    connection = (HttpURLConnection) new URL("http://localhost:"
                            + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();
                    connection.setDoOutput(false);
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("header1", "value1");
                    connection.setRequestProperty("header2", "");
                    connection.setRequestProperty("header3", "apple=orange");
                    connection.setConnectTimeout(20000);
                    connection.setReadTimeout(20000);

                    sendRequest(connection, requestSent);
                } catch (final Throwable t) {
                    if(connection != null ) {
                        try {
                            responseCode[0] = connection.getResponseCode();
                        } catch (IOException e) {
                            responseCode[0] = -1;
                        }
                    } else {
                        responseCode[0] = -2;
                    }
                } finally {
                    resultReady.countDown();
                }
            }
        });

        httpThread.start();
        runner.run(1, false, false);
        resultReady.await();

        runner.assertTransferCount(HandleHttpRequest.REL_SUCCESS, 0);
        assertEquals(503, responseCode[0]);
    }

    @Test
    public void testCleanup() throws Exception {
        // GIVEN
        int nrOfRequests = 5;

        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(nrOfRequests);
        CountDownLatch cleanupDone = new CountDownLatch(nrOfRequests-1);

        processor = new HandleHttpRequest() {
            @Override
            synchronized void initializeServer(ProcessContext context) throws Exception {
                super.initializeServer(context);
                serverReady.countDown();

                requestSent.await();
                while (getRequestQueueSize() < nrOfRequests) {
                    Thread.sleep(200);
                }
            }
        };

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        List<Response> responses = new ArrayList<>(nrOfRequests);
        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();

                    OkHttpClient client =
                        new OkHttpClient.Builder()
                            .readTimeout(3000, TimeUnit.MILLISECONDS)
                            .writeTimeout(3000, TimeUnit.MILLISECONDS)
                            .build();
                    client.dispatcher().setMaxRequests(nrOfRequests);
                    client.dispatcher().setMaxRequestsPerHost(nrOfRequests);

                    Callback callback = new Callback() {
                        @Override
                        public void onFailure(Call call, IOException e) {
                            // Will only happen once for the first non-rejected request, but not important
                        }

                        @Override
                        public void onResponse(Call call, Response response) throws IOException {
                            responses.add(response);
                            cleanupDone.countDown();
                        }
                    };
                    IntStream.rangeClosed(1, nrOfRequests).forEach(
                        requestCounter -> {
                            Request request = new Request.Builder()
                                .url(String.format("http://localhost:%s/my/" + requestCounter , port))
                                .get()
                                .build();
                            sendRequest(client, request, callback, requestSent);
                        }
                    );
                } catch (final Throwable t) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        // WHEN
        httpThread.start();
        runner.run(1, false);
        cleanupDone.await();

        // THEN
        int nrOfPendingRequests = processor.getRequestQueueSize();

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 1);

        assertEquals(1, contextMap.size());
        assertEquals(0, nrOfPendingRequests);
        assertEquals(responses.size(), nrOfRequests-1);
        for (Response response : responses) {
            assertEquals(HttpServletResponse.SC_SERVICE_UNAVAILABLE, response.code());
            assertTrue("Unexpected HTTP response for rejected requests", new String(response.body().bytes()).contains("Processor is shutting down"));
        }
    }

    @Test
    public void testSecure() throws Exception {
        secureTest(false);
    }

    @Test
    public void testSecureTwoWaySsl() throws Exception {
        secureTest(true);
    }

    private void secureTest(boolean twoWaySsl) throws Exception {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final Map<String, String> sslProperties = getServerKeystoreProperties();
        sslProperties.putAll(getTruststoreProperties());
        sslProperties.put(StandardSSLContextService.SSL_ALGORITHM.getName(), "TLSv1.2");
        useSSLContextService(runner, sslProperties, twoWaySsl ? SSLContextService.ClientAuth.WANT : SSLContextService.ClientAuth.NONE);

        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                    final HttpsURLConnection connection = (HttpsURLConnection) new URL("https://localhost:"
                            + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();

                    if (twoWaySsl) {
                        // use a client certificate, do not reuse the server's keystore
                        SSLContext clientSslContext = SslContextFactory.createSslContext(
                                getClientKeystoreProperties().get(StandardSSLContextService.KEYSTORE.getName()),
                                getClientKeystoreProperties().get(StandardSSLContextService.KEYSTORE_PASSWORD.getName()).toCharArray(),
                                "JKS",
                                getTruststoreProperties().get(StandardSSLContextService.TRUSTSTORE.getName()),
                                getTruststoreProperties().get(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName()).toCharArray(),
                                "JKS",
                                null,
                                "TLSv1.2");
                        connection.setSSLSocketFactory(clientSslContext.getSocketFactory());
                    } else {
                        // with one-way SSL, the client still needs a truststore
                        SSLContext clientSslContext = SslContextFactory.createTrustSslContext(
                                getTruststoreProperties().get(StandardSSLContextService.TRUSTSTORE.getName()),
                                getTruststoreProperties().get(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName()).toCharArray(),
                                "JKS",
                                "TLSv1.2");
                        connection.setSSLSocketFactory(clientSslContext.getSocketFactory());
                    }
                    connection.setDoOutput(false);
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("header1", "value1");
                    connection.setRequestProperty("header2", "");
                    connection.setRequestProperty("header3", "apple=orange");
                    connection.setConnectTimeout(3000);
                    connection.setReadTimeout(3000);

                    sendRequest(connection, requestSent);
                } catch (final Throwable t) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        httpThread.start();
        runner.run(1, false, false);

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 1);
        assertEquals(1, contextMap.size());

        final MockFlowFile mff = runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("http.query.param.query", "true");
        mff.assertAttributeEquals("http.query.param.value1", "value1");
        mff.assertAttributeEquals("http.query.param.value2", "");
        mff.assertAttributeEquals("http.query.param.value3", "");
        mff.assertAttributeEquals("http.query.param.value4", "apple=orange");
        mff.assertAttributeEquals("http.headers.header1", "value1");
        mff.assertAttributeEquals("http.headers.header3", "apple=orange");
        mff.assertAttributeEquals("http.protocol", "HTTP/1.1");
    }

    private HandleHttpRequest createProcessor(CountDownLatch serverReady, CountDownLatch requestSent) {
        return new HandleHttpRequest() {
            @Override
            synchronized void initializeServer(ProcessContext context) throws Exception {
                super.initializeServer(context);
                serverReady.countDown();

                requestSent.await();
                while (getRequestQueueSize()  == 0) {
                    Thread.sleep(200);
                }
            }

            @Override
            void rejectPendingRequests() {
                // Skip this, otherwise it would wait to make sure there are no more requests
            }
        };
    }

    private void sendRequest(HttpURLConnection connection, CountDownLatch requestSent) throws Exception {
        Future<InputStream> executionFuture = Executors.newSingleThreadExecutor()
            .submit(() -> connection.getInputStream());

        requestSent.countDown();

        executionFuture.get();
    }

    private void sendRequest(OkHttpClient client, Request request, CountDownLatch requestSent) {
        Callback callback = new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                // We (may) get a timeout as the processor doesn't answer unless there is some kind of error
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                // Not called as the processor doesn't answer unless there is some kind of error
            }
        };

        sendRequest(client, request, callback, requestSent);
    }

    private void sendRequest(OkHttpClient client, Request request, Callback callback, CountDownLatch requestSent) {
        client.newCall(request).enqueue(callback);
        requestSent.countDown();
    }

    private static class MockHttpContextMap extends AbstractControllerService implements HttpContextMap {

        private boolean registerSuccessfully = true;

        private final ConcurrentMap<String, HttpServletResponse> responseMap = new ConcurrentHashMap<>();

        @Override
        public boolean register(final String identifier, final HttpServletRequest request, final HttpServletResponse response, final AsyncContext context) {
            if(registerSuccessfully) {
                responseMap.put(identifier, response);
            }
            return registerSuccessfully;
        }

        @Override
        public HttpServletResponse getResponse(final String identifier) {
            return responseMap.get(identifier);
        }

        @Override
        public void complete(final String identifier) {
            responseMap.remove(identifier);
        }

        public int size() {
            return responseMap.size();
        }

        public boolean isRegisterSuccessfully() {
            return registerSuccessfully;
        }

        public void setRegisterSuccessfully(boolean registerSuccessfully) {
            this.registerSuccessfully = registerSuccessfully;
        }

        @Override
        public long getRequestTimeout(TimeUnit timeUnit) {
            return timeUnit.convert(30000, TimeUnit.MILLISECONDS);
        }
    }
}
