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
package org.apache.nifi.jms.processors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class ConsumeJMSIT {

    @Test
    public void validateSuccessfulConsumeAndTransferToSuccess() throws Exception {
        final String destinationName = "cooQueue";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            JMSPublisher sender = new JMSPublisher((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            final Map<String, String> senderAttributes = new HashMap<>();
            senderAttributes.put("filename", "message.txt");
            senderAttributes.put("attribute_from_sender", "some value");
            sender.publish(destinationName, "Hey dude!".getBytes(), senderAttributes);
            TestRunner runner = TestRunners.newTestRunner(new ConsumeJMS());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(jmsTemplate.getConnectionFactory());
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
            runner.setProperty(ConsumeJMS.DESTINATION, destinationName);
            runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.QUEUE);
            runner.run(1, false);
            //
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeExists(JmsHeaders.DESTINATION);
            successFF.assertAttributeEquals(JmsHeaders.DESTINATION, destinationName);
            successFF.assertAttributeExists("filename");
            successFF.assertAttributeEquals("filename", "message.txt");
            successFF.assertAttributeExists("attribute_from_sender");
            successFF.assertAttributeEquals("attribute_from_sender", "some value");
            successFF.assertContentEquals("Hey dude!".getBytes());
            String sourceDestination = successFF.getAttribute(ConsumeJMS.JMS_SOURCE_DESTINATION_NAME);
            assertNotNull(sourceDestination);
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    /**
     * Validates <a href="https://issues.apache.org/jira/browse/NIFI-6915">NIFI-6915</a>.
     * <p>
     * The test consists on:
     * <ul>
     * <li>Start a durable non shared consumer <tt>C1</tt> with client id <tt>client1</tt> subscribed to topic <tt>T</tt>.</li>
     * <li>Stop <tt>C1</tt>.</li>
     * <li>Publish a message <tt>M1</tt> to topic <tt>T</tt>.</li>
     * <li>Start <tt>C1</tt>.</li>
     * </ul>
     * It is expected <tt>C1</tt> receives message <tt>M1</tt>.
     * </p>
     * @throws Exception unexpected
     */
    @Test(timeout = 10000)
    public void validateNifi6915() throws Exception {
        BrokerService broker = new BrokerService();
        try {
            broker.setPersistent(false);
            broker.setBrokerName("broker1");
            broker.start();
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://broker1");
            final String destinationName = "validateNifi6915";

            TestRunner c1Consumer = createNonSharedDurableConsumer(cf, destinationName);
            // 1. Start a durable non shared consumer C1 with client id client1 subscribed to topic T.
            boolean stopConsumer = true;
            c1Consumer.run(1, stopConsumer);
            List<MockFlowFile> flowFiles = c1Consumer.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertTrue("Expected no messages", flowFiles.isEmpty());
            // 2. Publish a message M1 to topic T.
            publishAMessage(cf, destinationName, "Hi buddy!!");
            // 3. Start C1.
            c1Consumer.run(1, true);
            flowFiles = c1Consumer.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertEquals(1, flowFiles.size());

            // It is expected C1 receives message M1.
            final MockFlowFile successFF = flowFiles.get(0);
            assertNotNull(successFF);
            successFF.assertAttributeExists(JmsHeaders.DESTINATION);
            successFF.assertAttributeEquals(JmsHeaders.DESTINATION, destinationName);
            successFF.assertContentEquals("Hi buddy!!".getBytes());
            assertEquals(destinationName, successFF.getAttribute(ConsumeJMS.JMS_SOURCE_DESTINATION_NAME));
        } catch (Exception e) {
            throw e;
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    @Test(timeout = 10000)
    public void validateNifi6915OnlyOneThreadAllowed() {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        final String destinationName = "validateNifi6915";
        try {
            TestRunner runner = createNonSharedDurableConsumer(cf, destinationName);
            runner.setThreadCount(2);
            runner.run(1, true);
            fail();
        } catch (Throwable e) {
            // Unable to capture the message :(
        }

        TestRunner runner = createNonSharedDurableConsumer(cf, destinationName);
        // using one thread, it should not fail.
        runner.setThreadCount(1);
        runner.run(1, true);
    }

    private static void publishAMessage(ActiveMQConnectionFactory cf, final String destinationName, String messageContent) throws JMSException {
        // Publish a message.
        try (Connection conn = cf.createConnection();
                Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(session.createTopic(destinationName))) {
            producer.send(session.createTextMessage(messageContent));
        }
    }

    private static TestRunner createNonSharedDurableConsumer(ActiveMQConnectionFactory cf, final String destinationName) {
        ConsumeJMS c1 = new ConsumeJMS();
        TestRunner c1Consumer = TestRunners.newTestRunner(c1);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        try {
            c1Consumer.addControllerService("cfProvider", cs);
        } catch (InitializationException e) {
            throw new IllegalStateException(e);
        }
        c1Consumer.enableControllerService(cs);

        c1Consumer.setProperty(ConsumeJMS.CF_SERVICE, "cfProvider");
        c1Consumer.setProperty(ConsumeJMS.DESTINATION, destinationName);
        c1Consumer.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.TOPIC);
        c1Consumer.setProperty(ConsumeJMS.DURABLE_SUBSCRIBER, "true");
        c1Consumer.setProperty(ConsumeJMS.SUBSCRIPTION_NAME, "SubscriptionName");
        c1Consumer.setProperty(ConsumeJMS.SHARED_SUBSCRIBER, "false");
        c1Consumer.setProperty(ConsumeJMS.CLIENT_ID, "client1");
        return c1Consumer;
    }

}
