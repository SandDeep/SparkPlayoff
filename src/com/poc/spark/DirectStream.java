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
import org.bson.BSONObject;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import scala.Tuple2;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.hadoop.MongoOutputFormat;

/**
 * Direct Approach (No Receivers) - Use Kafka’s simple consumer API to Read data
 * 
 * @author Deepesh.Maheshwari
 *
 */
public class DirectStream {
	private static final Logger logger = Logger.getLogger(DirectStream.class);
	public static final long MINUTE_IN_MILLISEC = 300000l;
	public static final long HOUR_IN_MILLISEC = 300000*12l;
	public static final long DAY_IN_MILLISEC = 300000*12*24l;
			
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Spark Demo")
				.setMaster("local[2]").set("spark.executor.memory", "1g");

		StreamingContext ssc = new StreamingContext(conf, new Duration(20000));
		JavaStreamingContext jssc = new JavaStreamingContext(ssc);

		String brokers = "127.0.0.1:9092,127.0.0.1:9093";
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		//defaults to "largest"
		kafkaParams.put("auto.offset.reset", "smallest");

		Set<String> topicsSet = new HashSet<>();
		topicsSet.add("iBeat");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
		.createDirectStream(
							jssc, 
							String.class, 
							String.class,	
							StringDecoder.class, 
							StringDecoder.class, 
							kafkaParams,
							topicsSet);
		
		JavaDStream<Map<String, Object>> lines = directKafkaStream
				.map(new Function<Tuple2<String, String>, Map<String, Object>>() {

					private static final long serialVersionUID = 5196132687044875421L;

					@Override
					public Map<String, Object> call(Tuple2<String, String> tuple)
							throws Exception {
						logger.info(tuple._2());
						return convertToMap(tuple._2());
					}
				});

		JavaPairDStream<String, Integer> viewCount = lines
				.mapToPair(new PairFunction<Map<String, Object>, String, Integer>() {

					private static final long serialVersionUID = 5196132687044875422L;

					@Override
					public Tuple2<String, Integer> call(Map<String, Object> map)
							throws Exception {
						String host = (String) map.get("host");
						String articleId = (String) map.get("articleId");
						
						if(host==null || articleId==null){
							logger.error("*********** Error Doc ************\n"+map);
						}
						String key="articleId_"+articleId+"_host_"+host;
						logger.info(key);
						
						return new Tuple2<String, Integer>(key, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					
					private static final long serialVersionUID = 7028129084583344108L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1+v2;
					}
				});
		
		viewCount.print(100);
		
		/*JavaDStream<BSONObject> suspectedStream=viewCount.map(new Function<Tuple2<String,Integer>, BSONObject>() {

			@Override
			public BSONObject call(Tuple2<String, Integer> tuple) throws Exception {
				logger.info(tuple);
				DBObject dbObject = null;
				try {
					String[] arr=tuple._1.split("_");
					dbObject = BasicDBObjectBuilder.start()
							.add(arr[0],arr[1]).add(arr[2],arr[3])
							.add("count", tuple._2).get();
					logger.info(dbObject);
				} catch (Exception e) {
					logger.equals(tuple+"\n"+e.getMessage());
				}
				return dbObject;
			}
		});*/

		JavaPairDStream<Object, BSONObject> bsonCount=viewCount.mapToPair(new PairFunction<Tuple2<String,Integer>,Object, BSONObject>() {

			private static final long serialVersionUID = -3964716493263543805L;

			@Override
			public Tuple2<Object, BSONObject> call(Tuple2<String, Integer> tuple)
					throws Exception {
				logger.info(tuple);
						DBObject dbObject = null;
						try {
							String[] arr=tuple._1().split("_");
							dbObject = BasicDBObjectBuilder.start()
									.add(arr[0],arr[1]).add(arr[2],arr[3])
									.add("count", tuple._2()).get();
							logger.info(dbObject);
						} catch (Exception e) {
							logger.equals(tuple+"\n"+e.getMessage());
						}
				// null key means an ObjectId will be generated on insert
				return new Tuple2<Object, BSONObject>(null, dbObject);
			}
		});
		
		//bsonCount.print();
		//suspectedStream.print();
		final Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/test.spark");
		outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
		
		/*suspectedStream.foreach(new Function<JavaRDD<BSONObject>, Void>() {
			
			private static final long serialVersionUID = 4414703053334523053L;

			@Override
			public Void call(JavaRDD<BSONObject> rdd) throws Exception {

				try {
					logger.info(rdd.first());
					rdd.saveAsObjectFile("E://");
					for (Object entry : rdd.collect().toArray()) {
						logger.info(entry);
					}
					rdd.saveAsTextFile("E://");
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				return null;
			}
		});*/
		
		bsonCount.foreachRDD(new Function<JavaPairRDD<Object,BSONObject>, Void>() {
			
			private static final long serialVersionUID = -5447455949365193895L;

			@Override
			public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
				//rdd.saveAsTextFile("E://spark.log");
				rdd.saveAsNewAPIHadoopFile("file:///notapplicable",
			            Object.class, BSONObject.class, MongoOutputFormat.class, outputConfig);
				
				return null;
			}
		});
		
		jssc.start();
		jssc.awaitTermination();
				
	}
	
	@SuppressWarnings("unused")
	private static void saveOutput(JavaPairDStream<Object, BSONObject> bsonCount) {
		final Configuration outputConfig  =new Configuration();
		outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/output.collection");
		outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
		
		bsonCount.foreachRDD(new Function<JavaPairRDD<Object,BSONObject>, Void>() {
			
			private static final long serialVersionUID = -5447455949365193895L;

			@Override
			public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
				logger.info(rdd.toString());
				rdd.saveAsNewAPIHadoopFile("", Object.class, BSONObject.class, BSONFileOutputFormat.class,outputConfig);
				
				return null;
			}
		});
		
	}

	private static Map<String, Object> convertToMap(String msg) {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> map = new HashMap<>();

		try {
			map = mapper.readValue(msg,new TypeReference<HashMap<String, Object>>() {});
			logger.info(map);
			
		} catch (IOException e) {
			logger.error(e.getMessage());
			return null;
		}
		return map;
	}
}
