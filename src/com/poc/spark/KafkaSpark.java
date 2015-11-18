package com.poc.spark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache
 * /spark/examples/streaming/JavaKafkaWordCount.java
 * 
 * @author Deepesh.Maheshwari
 *
 */
public class KafkaSpark {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final Logger logger = Logger.getLogger(KafkaSpark.class);
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Spark Demo").setMaster(
				"local[2]"/*"spark://iBeatMongoDB3452:7077"*/).set("spark.executor.memory", "1g");
		
		StreamingContext ssc = new StreamingContext(conf, new Duration(12000));
		JavaStreamingContext jssc = new JavaStreamingContext(ssc);

		/******************* Receiver-based Approach ********************/
		
		// where to find instance of Zookeeper in your cluster.
		//String zkQuorum = "192.168.34.53:2181";
		/*String zkQuorum = "localhost:2181";
		
		// Consumer Group this process is consuming on behalf of.
		String groupId = "siteUser";

		// per-topic number of Kafka partitions to consume
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("ibeat_pagetrendLog", 2);

		// Approach 1: Receiver-based Approach - Use  Kafka’s high level API to Read data
		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils
				.createStream(jssc, zkQuorum, groupId, topicMap,StorageLevel.MEMORY_AND_DISK());*/

		
		/******************* Direct Approach ********************/
		
		//String brokers = "192.168.34.53:9092,192.168.34.52:9093";
		String brokers = "127.0.0.1:9092,127.0.0.1:9093";
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		//defaults to "largest"
		kafkaParams.put("auto.offset.reset", "smallest");

		Set<String> topicsSet = new HashSet<>();
		topicsSet.add("ibeat_pagetrendLog");

		// Approach 2: Direct Approach (No Receivers) - Use Kafka’s simple
		// consumer API to Read data
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
		.createDirectStream(
							jssc, 
							String.class, 
							String.class,	
							StringDecoder.class, 
							StringDecoder.class, 
							kafkaParams,
							topicsSet);
		
		// for Zookeeper Update
		/*directKafkaStream
				.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {

					private static final long serialVersionUID = -1001922816585812888L;

					@Override
					public Void call(JavaPairRDD<String, String> rdd) throws Exception {
						OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd).offsetRanges();
						System.out.println(offsetRanges[0]);
						return null;
					}
				});*/
		
		JavaDStream<String> lines=directKafkaStream.map(new Function<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = 1294614499027788781L;

			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				logger.info(tuple2);
				return tuple2._2;
			}
		});
		
		JavaDStream<String> words=lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = -2042174881679341118L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				logger.info(Lists.newArrayList(SPACE.split(t)));
				logger.info(t);
				return Lists.newArrayList(SPACE.split(t));
			}
		});
		
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 4933101070430638514L;

						@Override
						public Tuple2<String, Integer> call(String t)
							throws Exception {
						return new Tuple2<String, Integer>(t, 1);
						}
					}).reduceByKey(new Function2<Integer, Integer, Integer>() {
						private static final long serialVersionUID = 8871129379831995209L;

						@Override
						public Integer call(Integer v1, Integer v2) throws Exception {
							return v1 + v2;
						}
					});
		
		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();
		//jssc.stop();
	}
}
