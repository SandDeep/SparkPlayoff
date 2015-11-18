package com.poc.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerGroupExample {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public ConsumerGroupExample(String aZookeeper, String aGroupId, String topic) {
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(
				aZookeeper, aGroupId));
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper,
			String a_groupId) {
		Properties props = new Properties();

		// where to find instance of Zookeeper in your cluster.
		props.put("zookeeper.connect", a_zookeeper);

		// Consumer Group this process is consuming on behalf of.
		props.put("group.id", a_groupId);

		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	private void shutdown() {
		if (consumer != null) {
			consumer.shutdown();
		}

		if (executor != null) {
			executor.shutdown();
		}

		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out
						.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out
					.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void run(int thread) {
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, thread);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		executor = Executors.newFixedThreadPool(thread);

		int threadNumber = 0;

		for (KafkaStream<byte[], byte[]> kafkaStream : streams) {
			executor.submit(new ConsumerTest(kafkaStream, threadNumber++));
		}
	}

	public static void main(String[] args) {
		//String zooKeeper = "192.168.34.53:2181";
		String zooKeeper = "localhost:2181";
		String groupId = "siteUser";
		String topic = "ibeat_pagetrendLog";
		int threads = 2;

		ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper,
				groupId, topic);
		example.run(threads);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		example.shutdown();
	}
}
