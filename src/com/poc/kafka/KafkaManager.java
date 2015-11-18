package com.poc.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.poc.mongo.MongoUtil;
import com.poc.util.Config;

public class KafkaManager implements Serializable{

	private static final long serialVersionUID = -6247519812098892210L;

	private static final Logger logger = Logger.getLogger(KafkaManager.class);

	private Map<String, String> kafkaParams;

	public KafkaManager(Map<String, String> kafkaParams) {
		this.kafkaParams = kafkaParams;
	}

	public Map<TopicAndPartition, Long> getlastCommitedOffset(String topicName) {

		DBCollection collection = MongoUtil.getCollection(Config.DBNAME, Config.OFFSET_COLLECTION);

		DBCursor cursor = collection.find(new BasicDBObject("topic", topicName));

		// No Documents Returned
		if (cursor.size() == 0) {
			return null;
		}

		Map<TopicAndPartition, Long> mapOffset = new HashMap<>();
		Map<Integer, DBObject> partitionMap = new HashMap<>();

		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			int partition = (int) obj.get("partition");
			partitionMap.put(partition, obj);
		}

		for (Integer partitionKey : partitionMap.keySet()) {
			DBObject obj = partitionMap.get(partitionKey);

			//String topicName = (String) obj.get("topic");
			int partition = (int) obj.get("partition");
			Long untilOffset = (Long) obj.get("untilOffset");

			mapOffset.put(new TopicAndPartition(topicName, partition),
					untilOffset);
		}
		return mapOffset;
	}

	public JavaInputDStream<String> getKafkaDirectStream(
			JavaStreamingContext jssc, String topicName) {
		Map<TopicAndPartition, Long> kafkaOffsetMap = getlastCommitedOffset(topicName);

		DirectStreamChain chain1=new PairDStreamProcessor();
		DirectStreamChain chain2=new DStreamProcessor();
		chain1.setNext(chain2);
		chain1.process(kafkaOffsetMap);
		if (kafkaOffsetMap == null) {
	
		}

		return KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, String.class,
				kafkaParams, kafkaOffsetMap,
				new Function<MessageAndMetadata<String, String>, String>() {

					private static final long serialVersionUID = 5634434565638417704L;

					@Override
					public String call(MessageAndMetadata<String, String> tuple)
							throws Exception {
						/*logger.info("Key : " + tuple.key());
						logger.info("Msg : " + tuple.message());
						logger.info("Offset : " + tuple.offset());
						logger.info("TopicPart : " + tuple.partition() + " : "
								+ tuple.topic());
						logger.info(tuple.productArity() + " : "
								+ tuple.productPrefix());
						logger.info("Key Decoder : " + tuple.keyDecoder());*/
						return tuple.message();
					}

				});
	}

	public static void main(String[] args) {
		DBCollection collection = MongoUtil.getCollection(Config.DBNAME,
				Config.OFFSET_COLLECTION/*"test","kafka"*/);

		BasicDBObject sortObj = new BasicDBObject("$natural", -1);
		DBCursor cursor = collection.find().sort(sortObj);

		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			//String topicName = (String) obj.get("topic");
			int partition = (int) obj.get("partition");
			//Long untilOffset = (Long) obj.get("untilOffset");
		}
		System.out.println();
	}
}
