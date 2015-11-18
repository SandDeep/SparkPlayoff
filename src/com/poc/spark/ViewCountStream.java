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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoOutputFormat;

/**
 * Direct Approach (No Receivers) - Use Kafka’s simple consumer API to Read data
 * 
 * @author Deepesh.Maheshwari
 *
 */
public class ViewCountStream {
	private static final Logger logger = Logger.getLogger(ViewCountStream.class);

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
		topicsSet.add("iBeat");//_pagetrendLog

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
						return convertToMap(tuple._2);
					}
				});

		JavaPairDStream<String, Map<String, Object>> viewCount = lines
				.mapToPair(new PairFunction<Map<String, Object>, String, Map<String, Object>>() {

					private static final long serialVersionUID = 5196132687044875422L;

					@Override
					public Tuple2<String, Map<String, Object>> call(Map<String, Object> map)
							throws Exception {
						String host = (String) map.get("host");
						String articleId = (String) map.get("articleId");
						
						if(host==null || articleId==null){
							logger.error("*********** Error Doc ************\n"+map);
						}
						String key="articleId_"+articleId+"_host_"+host;
						logger.info(key);
						map.put("count", 1);
						
						return new Tuple2<String, Map<String, Object>>(key, map);
					}
				}).reduceByKey(new Function2<Map<String,Object>, Map<String,Object>, Map<String,Object>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, Object> call(Map<String, Object> map1,
							Map<String, Object> map2) throws Exception {
						int count1=(int) map1.get("count");
						int count2=(int) map2.get("count");
						
						map1.put("count", count1+count2);
						return map1;
					}
				});
		
		viewCount.print(100);
		
		JavaPairDStream<Object, BSONObject> bsonCount=viewCount.mapToPair(new PairFunction<Tuple2<String,Map<String, Object>>,Object, BSONObject>() {

			private static final long serialVersionUID = -3964716493263543805L;

			@Override
			public Tuple2<Object, BSONObject> call(Tuple2<String, Map<String, Object>> tuple)
					throws Exception {
				logger.info(tuple);
						DBObject dbObject = null;
						try {
							/*String[] arr=tuple._1.split("_");
							dbObject = BasicDBObjectBuilder.start()
									.add(arr[0],arr[1]).add(arr[2],arr[3])
									.add("count", tuple._2).get();*/
							
							dbObject=new BasicDBObject(tuple._2);
							
							logger.info(dbObject);
						} catch (Exception e) {
							logger.equals(tuple+"\n"+e.getMessage());
						}
				// null key means an ObjectId will be generated on insert
				return new Tuple2<Object, BSONObject>(null, dbObject);
			}
		});
		
		final Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/test.spark1");
		outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
		
		bsonCount.foreachRDD(new Function<JavaPairRDD<Object,BSONObject>, Void>() {
			
			private static final long serialVersionUID = -5447455949365193895L;

			@Override
			public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
				rdd.saveAsNewAPIHadoopFile("file:///notapplicable",
			            Object.class, BSONObject.class, MongoOutputFormat.class, outputConfig);
				
				return null;
			}
		});
		
		jssc.start();
		jssc.awaitTermination();
				
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
