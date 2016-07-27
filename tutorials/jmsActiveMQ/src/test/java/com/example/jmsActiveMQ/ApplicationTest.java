/**
 * Put your copyright and license info here.
 */
package com.example.jmsActiveMQ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.datatorrent.api.LocalMode;
import com.example.jmsActiveMQ.ActiveMQApplication;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  
  private static final String FILE_NAME = "test";
  private static final String FILE_DIR  = "/tmp/FromAMQ";
  private static final String FILE_PATH = FILE_DIR + "/" + FILE_NAME + ".0";     // first part
  
  private static final String QUEUE_NAME = "jms4Amq";
  
  private Connection connection;
  private MessageProducer producer;
  private Session session;
  
  // test messages                                                                                                                                
  private static String[] lines =
  {
    "1st line",
    "2nd line",
    "3rd line",
    "4th line",
    "5th line",
  };
  
  @Test
  public void testApplication() throws IOException, Exception {
    try {
      // delete output file if it exists                                                                                                          
      File file = new File(FILE_PATH);
      file.delete();
      
      createAMQClient();
      
      // write messages to SQS Queue                                                                                                            
      writeToQueue();

      // run app asynchronously; terminate after results are checked                                                                              
      LocalMode.Controller lc = asyncRun();

      // check for presence of output file                                                                                                        
      chkOutput();

      // compare output lines to input                                                                                                            
      compare();
      
      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void createAMQClient() throws JMSException 
  {
    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

    // Create a Connection
    connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

    // Create the destination (Topic or Queue)
    Destination destination = session.createQueue(QUEUE_NAME);

    // Create a MessageProducer from the Session to the Topic or Queue
    producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  private void writeMsg(String[] msgs) throws JMSException 
  {
    for (String text : msgs) {
      TextMessage message = session.createTextMessage(text);
      producer.send(message);
    }
  }
  
  private void writeToQueue() throws JMSException {
    writeMsg(lines);
    
    // Clean up
    session.close();
    connection.close();
    
    LOG.debug("Sent messages to topic {}", QUEUE_NAME);
  }
  
  private Configuration getConfig() {
    Configuration conf = new Configuration(false);
    conf.set(ActiveMQApplication.QUEUE_NAME_PROPERTY, QUEUE_NAME);

    String pre = "dt.operator.fileOut.prop.";
    conf.set(   pre + "filePath",        FILE_DIR);
    conf.set(   pre + "baseName",        FILE_NAME);
    conf.setInt(pre + "maxLength",       50);
    conf.setInt(pre + "rotationWindows", 10);

    return conf;
  }

  private static void chkOutput() throws Exception {
    File file = new File(FILE_PATH);
    final int MAX = 60;
    for (int i = 0; i < MAX && (! file.exists()); ++i ) {
      LOG.debug("Sleeping, i = {}", i);
      Thread.sleep(1000);
    }
    if (! file.exists()) {
      String msg = String.format("Error: %s not found after %d seconds%n", FILE_PATH, MAX);
      throw new RuntimeException(msg);
    }
  }
  
  private void compare() throws Exception {
    // read output file                                                                                                                           
    File file = new File(FILE_PATH);
    BufferedReader br = new BufferedReader(new FileReader(file));

    HashSet<String> set = new HashSet<String>();
    String line;
    while (null != (line = br.readLine())) {
      set.add(line);
    }
    br.close();

    // now delete the file, we don't need it anymore
    Assert.assertTrue("Deleting "+file, file.delete());

    // compare                                                                                                                                    
    Assert.assertEquals("number of lines", lines.length, set.size());
    for (int i = 0; i < lines.length; ++i) {
      Assert.assertTrue("set contains "+lines[i], set.remove(lines[i]));
    }
  }

  private LocalMode.Controller asyncRun() throws Exception {
    Configuration conf = getConfig();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new ActiveMQApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }
  
}
