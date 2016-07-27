/**
 * Put your copyright and license info here.
 */
package com.example.jmsActiveMQ;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.jms.JMSStringInputOperator;

@ApplicationAnnotation(name="Amq2HDFS")
public class ActiveMQApplication implements StreamingApplication
{

  /**
   * Queue name to use 
   */
  public static final String QUEUE_NAME_PROPERTY = "AMQ_QUEUE_NAME";
	
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JMSStringInputOperator amqInput = dag.addOperator("amqIn", 
        new JMSStringInputOperator());

    amqInput.setSubject(conf.get(QUEUE_NAME_PROPERTY));
    amqInput.getConnectionFactoryProperties().put("brokerURL", "vm://localhost");
    
    LineOutputOperator out = dag.addOperator("fileOut", new LineOutputOperator());

    dag.addStream("data", amqInput.output, out.input);
  }
}
