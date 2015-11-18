package com.poc.util;

public final class KafkaProperties {
	public static String ZOOKEEPER_SERVER_ADDRESS = "192.168.34.53:2181";
	public static String LOCAL_ZOOKEEPER_SERVER_ADDRESS = "localhost:2181";
	
	public static String BROKER_LIST = "192.168.34.53:9092,192.168.34.52:9093";
	public static String LOCAL_BROKER_LIST = "127.0.0.1:9092,127.0.0.1:9093";
	
	public static String KEY_SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
	public static String PARTITIONER_CLASS = "com.til.shard.SimplePartitioner";
	public static String PRODUCER_TYPE = "async";
	public static String RRACKS = "1";
	
	public static String KAFKA_TOPIC_NAME = "iBeat";
}
