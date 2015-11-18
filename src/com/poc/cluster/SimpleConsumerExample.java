package com.poc.cluster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

public class SimpleConsumerExample {

	private static final Logger logger = Logger.getLogger(SimpleConsumerExample.class);
	List<String> m_replicaBrokers = new ArrayList<>();
	
	public static void main(String[] args) {
		SimpleConsumerExample consumer = new SimpleConsumerExample();

		long maxReads = 1;
		String topic = "ibeat_pagetrendLog";
		int partition = 1;
		List<String> seedBrokers = new ArrayList<>();
		//seedBrokers.add("192.168.34.53");
		seedBrokers.add("localhost");
		int port = 9093;

		try {
			consumer.run(maxReads, topic, partition, seedBrokers, port);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	private void run(long maxReads, String topic, int partition,
			List<String> seedBrokers, int port) throws Exception {
		PartitionMetadata metadata = findLeader(seedBrokers, port, topic,
				partition);

		if (metadata == null) {
			logger.info("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			logger.info("Can't find Leader for Topic and Partition. Exiting");
			return;
		}

		String leadBroker = metadata.leader().host();
		int soTimeout = 10000;
		int bufferSize = 64 * 1024;
		String clientName = "Client_" + topic + "_" + partition;

		SimpleConsumer consumer = new SimpleConsumer(leadBroker, metadata.leader().port(),
				soTimeout, bufferSize, clientName);

		long readOffset = getLastOffset(consumer, topic, partition,
				kafka.api.OffsetRequest.EarliestTime(), clientName);
		int numErrors = 0;

		while (maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), soTimeout,
						bufferSize, clientName);
			}

			/*
			 * Note: this fetchSize of 100000 might need to be increased if
			 * large batches are written to Kafka
			 */
			kafka.api.FetchRequest req = new FetchRequestBuilder()
					.clientId(clientName)
					.addFetch(topic, partition, readOffset, 10000).build();

			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {
				numErrors++;
				short code = fetchResponse.errorCode(topic, partition);
				System.out.println("Error fetching data from the Broker:"
						+ leadBroker + " Reason: " + code);
				if (numErrors > 5)
					break;

				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for
					// the last element to reset
					readOffset = getLastOffset(consumer, topic, partition,
							kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}

				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, topic, partition, port);
				continue;
			}
			numErrors = 0;

			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
					topic, partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					logger.info("Found an old offset: " + currentOffset
							+ " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();

				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset())
						+ ": " + new String(bytes, "UTF-8"));
				numRead++;
				maxReads--;
			}

			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}

		if (consumer != null) {
			consumer.close();
		}
	}

	private String findNewLeader(String oldLeader, String topic, int partition,
			int port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, port,
					topic, partition);

			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (oldLeader.equalsIgnoreCase(metadata.leader().host())
					&& i == 0) {
				/**
				 * first time through if the leader hasn't changed give
				 * ZooKeeper a second to recover second time, assume the broker
				 * did recover before failover, or it was a non-Broker issue
				 */
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		logger.info("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	private long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		
		TopicAndPartition topicAndPartition=new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo=new HashMap<>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		
		OffsetRequest req=new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse res=consumer.getOffsetsBefore(req);
		
		if(res.hasError()){
			logger.error("Error fetching data Offset Data the Broker. Reason: " +res.errorCode(topic, partition));
			return 0;
		}
		
		long[] offsets=res.offsets(topic, partition);
		return offsets[0];
	}

	private PartitionMetadata findLeader(List<String> seedBrokers, int port,
			String topic, int partition) {
		PartitionMetadata returnMetaData = null;

		loop: for (String seed : seedBrokers) {
			SimpleConsumer consumer = null;
			int soTimeout = 10000;
			int bufferSize = 64 * 1024;
			String clientId = "leaderLookup";

			try {
				consumer = new SimpleConsumer(seed, port, soTimeout,
						bufferSize, clientId);

				List<String> topics = Collections.singletonList(topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				TopicMetadataResponse res = consumer.send(req);

				List<TopicMetadata> metadata = res.topicsMetadata();

				for (TopicMetadata item : metadata) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}

			} catch (Exception e) {
				logger.info(e.getMessage());
				logger.info("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + topic + ", " + partition
						+ "] Reason: " + e);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}

		if (returnMetaData != null) {
			for (Broker replica : returnMetaData.replicas()) {
				logger.info(replica.id() + " -> " + replica.host() + ":"
						+ replica.port());
				m_replicaBrokers.add(replica.host());
			}
		}

		logger.info("Broker List : " + m_replicaBrokers);
		logger.info(returnMetaData);
		return returnMetaData;
	}
}
