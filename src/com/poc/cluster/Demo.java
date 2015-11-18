package com.poc.cluster;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

public class Demo {
	
	// private Log log = LogFactory.getLog(getClass());
	private static final Logger logger = Logger.getLogger(Demo.class);
	
	public static void main(String[] args) {
		Demo client = new Demo();

		client.createTopic("page_visits");
		
		int numEvents=10000;
		client.produceData("page_visits",numEvents);
	}

	private void produceData(String topicName, int numEvents) {
		Properties props = new Properties();

		/*
		 * Producer can find a one or more Brokers to determine the Leader for
		 * each topic. This does not need to be the full set of Brokers in your
		 * cluster but should include at least two in case the first Broker is
		 * not available
		 */
		props.put("metadata.broker.list",
				"192.168.34.53:9092,192.168.34.52:9093");

		// Serializer to use when preparing the message for transmission to the
		// Broker
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		/*
		 * what class to use to determine which Partition in the Topic the
		 * message is to be sent to
		 */
		/*
		 * If you include a value for the key but haven't defined a
		 * partitioner.class Kafka will use the default partitioner. If the key
		 * is null, then the Producer will assign the message to a random
		 * Partition
		 */
		//props.put("partitioner.class", "com.cluster.SimplePartitioner");

		/*
		 * tells Kafka that you want your Producer to require an acknowledgement
		 * from the Broker that the message was received.
		 */
		props.put("request.required.acks", "1");
		props.put("producer.type", "async");
		
		ProducerConfig config = new ProducerConfig(props);

		/*
		 * The first is the type of the Partition key, the second the type of
		 * the message
		 */
		Producer<String, String> producer = new Producer<>(config);
		
		Random rnd=new Random();
		
		for (int i = 0; i < numEvents; i++) {
			long runtime=new Date().getTime();
			String ip = "192.168.2." + rnd.nextInt(255); 
            String msg = runtime + ",www.ibeat.com," + ip; 
			KeyedMessage<String, String> data = new KeyedMessage<>(topicName, ip, msg);
			producer.send(data);
		}
		
		producer.close();
	}

	private void createTopic(String topicName) {

		// Create a ZooKeeper client
		String zkServers = "192.168.34.53:2181";
		int sessionTimeoutMs = 10000;
		int connectionTimeoutMs = 10000;
		ZkClient zkClient = new ZkClient(zkServers, sessionTimeoutMs,
				connectionTimeoutMs, ZKStringSerializer$.MODULE$);
		// ZkSerializer serializer=new

		// Create a topic named topicName with 5 partitions and a replication
		// factor of 3
		int numPartitions = 2;
		int replicationFactor = 2;
		Properties topicConfig = new Properties();
		try {
			AdminUtils.createTopic(zkClient, topicName, numPartitions,
					replicationFactor, topicConfig);
		} catch (Exception e) {
			logger.debug(e.getMessage());
		}
	}
}
