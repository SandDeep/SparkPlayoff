package com.poc.spark;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.mongodb.hadoop.MongoOutputFormat;
import com.poc.util.Config;
import com.poc.util.KafkaProperties;
import com.poc.util.SparkProperties;

/**
 * Direct Approach (No Receivers) - Use Kafka’s simple consumer API to Read data
 * 
 * @author Deepesh.Maheshwari
 *
 */
public class MongoHadoop {

	private static final Logger logger = Logger.getLogger(MongoHadoop.class);
	private static JavaStreamingContext jssc;
	private static Configuration config;
	
	public static void main(String[] args) {
		
		//JavaPairInputDStream<String, String> directKafkaStream = setupSpark();
		
		config = new Configuration();
		
		//Input Config
		config.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		config.set("mongo.input.uri",SparkProperties./*LOCAL_*/MONGO_OUTPUT_URI);
		config.set("mongo.input.query","{host: 'timesofindia.indiatimes.com'}");
		
		//Output Config
		config.set("mongo.output.uri", SparkProperties./*LOCAL_*/MONGO_OUTPUT_URI);
		config.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
		
		JavaSparkContext sc=new JavaSparkContext("local", "MongoOps");

		JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config,
				com.mongodb.hadoop.MongoInputFormat.class, Object.class,
				BSONObject.class);
		
		mongoRDD.count();
		sc.close();
		//saveToDB(bsonCount);
		//jssc.start();
		//jssc.awaitTermination();
				
	}
	
	private static JavaPairInputDStream<String, String> setupSpark() {
		SparkConf conf = new SparkConf().setAppName("Spark Aggregation")
				.setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", SparkProperties.EXECUTOR_MEMORY);

		StreamingContext ssc = new StreamingContext(conf, new Duration(Config.FIVE_MINUTE_IN_MILLISEC));
		jssc = new JavaStreamingContext(ssc);

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list",
				KafkaProperties./*LOCAL_*/BROKER_LIST);

		// defaults to "largest"
		//kafkaParams.put("auto.offset.reset", SparkProperties.AUTO_OFFSET_RESET);

		Set<String> topicsSet = new HashSet<>();
		topicsSet.add(KafkaProperties.KAFKA_TOPIC_NAME);

		return KafkaUtils.createDirectStream(
						jssc,
						String.class,
						String.class,
						StringDecoder.class,
						StringDecoder.class,
						kafkaParams,
						topicsSet);
	}

	private static void saveToDB(JavaPairDStream<Object, BSONObject> bsonCount) {
			
			bsonCount.foreachRDD(new Function<JavaPairRDD<Object,BSONObject>, Void>() {
				
				private static final long serialVersionUID = -5447455949365193895L;
	
				@Override
				public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
					//TODO Write Concern 0{un-ack}
					rdd.saveAsNewAPIHadoopFile("file:///notapplicable",
				            Object.class, BSONObject.class, MongoOutputFormat.class, config);
					
					return null;
				}
			});
		}

	private static Map<String, Object> convertToMap(String msg) {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> map;

		try {
			map = mapper.readValue(msg,new TypeReference<HashMap<String, Object>>() {});
			logger.debug(map);
			
		} catch (IOException e) {
			logger.error(e.getMessage());
			return null;
		}
		return map;
	}
}
