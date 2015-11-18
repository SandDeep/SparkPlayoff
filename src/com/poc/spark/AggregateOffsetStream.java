package com.poc.spark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import kafka.common.TopicAndPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.bson.BSONObject;

import scala.Tuple2;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoOutputFormat;
import com.poc.ibeat.util.DateTimeUtils;
import com.poc.kafka.KafkaManager;
import com.poc.mongo.MongoUtil;
import com.poc.util.Config;
import com.poc.util.HelperUtil;
import com.poc.util.KafkaProperties;
import com.poc.util.SparkProperties;

/**
 * Direct Approach (No Receivers) - Use Kafka’s simple consumer API to Read data
 * 
 * @author Deepesh.Maheshwari
 *
 */
public class AggregateOffsetStream {

	private static final Logger logger = Logger.getLogger(AggregateOffsetStream.class);
	private static JavaStreamingContext jssc;
	private static final AtomicReference<OffsetRange[]>  offsetRanges = new AtomicReference<>();
	
	public static void main(String[] args) {
		
		JavaInputDStream<String> directKafkaStream = setupSpark();
		JavaPairDStream<Object, BSONObject> bsonCount=groupByMin(directKafkaStream);
		saveToDB(bsonCount);
		jssc.start();
		jssc.awaitTermination();
				
	}
	
	private static JavaPairDStream<Object, BSONObject> groupByMin(
			JavaInputDStream<String> directKafkaStream) {

		//Update Kafka Offset
		saveKafkaOffset(directKafkaStream);
		
		final Long startTs = DateTimeUtils.get5MinuteSliceStamp(5);
		
		JavaPairDStream<String, StreamObject> viewCount=directKafkaStream.mapToPair(new PairFunction<String, String,StreamObject>() {
			private static final long serialVersionUID = -556686240923635777L;
			
			@SuppressWarnings("unchecked")
			@Override
			public Tuple2<String, StreamObject> call(String data) throws Exception {
				logger.debug(data);

				Map<String, Object> object = HelperUtil.convertToMap(data);
				Map<String, Object> map = new HashMap<>();
				StreamObject streamObject=new StreamObject();
				String key="";
				
				try {
					if (object != null) {


						String articleId = (String) object.get("articleId");
						//Long count        	= ((Double)(Double.parseDouble((object.get("count")).toString()))).longValue();
						String cat = (String) object.get("cat");
						String host = (String) object.get("host");
						Long isAPP = 0l;
						try {
							isAPP = ((Double) (Double.parseDouble((object.get("isAPP")).toString()))).longValue();
						} catch (Exception e) {
							logger.debug(isAPP + " " + e.getMessage());
						}
						Long contentType = 1l;
						try {
							contentType = ((Double) (Double.parseDouble((object.get("contentType")).toString()))).longValue();
						} catch (Exception e) {
							logger.debug(contentType + " " + e.getMessage());
						}
						Boolean isGroup = false;
						try {
							isGroup = Boolean.parseBoolean(object.get("isGroup").toString());
						} catch (Exception e) {
							logger.debug(isGroup + " " + e.getMessage());
						}

						String url = (String) ((object.get("url")));
						Long catIdHash = (Long) (object.get("catIdHash"));

						long publishTime = 0L;
						if (((object.get("publishTime"))) != null) {
							publishTime = ((Double) Double.parseDouble(object.get("publishTime").toString())).longValue();
						}
						String author = "";
						if (((object.get("author"))) != null) {
							author = ((object.get("author"))).toString();
						}
						String channel = "";
						if (((object.get("channel"))) != null) {
							channel = ((object.get("channel"))).toString();
						}

						List<String> tagsList = (List<String>) object.get("tags");
						//List<String> catsList = (List<String>) map.get("catIds");
						List<String> subsList = (List<String>) object.get("subcat");

						// populate DataMap
						map.put("articleId", articleId);
						map.put("host", host);
						map.put("isAPP", isAPP);
						map.put("contentType", contentType);
						map.put("cat", cat);
						map.put("subcat", subsList);
						map.put("url", url);
						map.put("publishTime", publishTime);
						map.put("author", author);
						map.put("channel", channel);
						map.put("timeStamp", startTs);
						//map.put("tags", tagsList);
						map.put("catIds", object.get("catIds")/* catsList */);
						map.put("isGroup", isGroup);
						map.put("catIdHash", catIdHash);

						if (host == null || articleId == null) {
                            logger.debug("*********** Error Doc ************\n" + map);
                        }
                        key = "articleId_" + articleId+ "_host_" + host+ "_isAPP_" + isAPP;

                        streamObject.setKey(key);
                        streamObject.setCount(1L);
                        streamObject.setMap(map);
                        streamObject.setTags(new HashSet<>(tagsList));
					}
				} catch (Exception e) {
					System.out.println(e.getMessage());
					//System.out.println(Config.getStackTrace(e));
				}
				return new Tuple2<>(key, streamObject);
			}
		}).reduceByKey(new Function2<StreamObject, StreamObject, StreamObject>() {
			
			private static final long serialVersionUID = 6528000178718055033L;

			@Override
			public StreamObject call(StreamObject s1, StreamObject s2) throws Exception {
				Long count1 = s1.getCount();
				Long count2 = s2.getCount();
			
				//Merge Sets
				Set<String> tags1=s1.getTags();
				Set<String> tags2=s2.getTags();
				
				tags1.addAll(tags2);
				
				s1.setTags(tags1);
				s1.setCount(count1+count2);
				
				return s1;
			}
		});
		
		viewCount.print(20);

		return viewCount.mapToPair(new PairFunction<Tuple2<String,StreamObject>, Object, BSONObject>() {

			private static final long serialVersionUID = 6144855830933465260L;

			@Override
			public Tuple2<Object, BSONObject> call(
					Tuple2<String, StreamObject> tuple) throws Exception {
				DBObject dbObject = null;
				try {

					StreamObject sObj=tuple._2();
					Map<String, Object> map=sObj.getMap();
					map.put("count", sObj.getCount());
					map.put("tags", sObj.getTags());
					
					dbObject = new BasicDBObject(map);

					//logger.info(dbObject);
				} catch (Exception e) {
					logger.debug(tuple + "\n" + e.getMessage());
				}

				// null key means an ObjectId will be generated on insert
				return new Tuple2<Object, BSONObject>(null, dbObject);
			}
		});
		}
	
	/**
	 * Process and Save kafka Offset
	 * 
	 * @param directKafkaStream
	 */
	private static void saveKafkaOffset(JavaInputDStream<String> directKafkaStream) {
		
		directKafkaStream.transformToPair(new Function<JavaRDD<String>, JavaPairRDD<String, String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public JavaPairRDD<String, String> call(JavaRDD<String> rdd)
					throws Exception {
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);

				return rdd.mapToPair(new PairFunction<String, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(String data)
							throws Exception {
						return new Tuple2<String, String>(null,data);
					}
				});
			}
		}).foreachRDD(
				new Function<JavaPairRDD<String, String>, Void>() {

					private static final long serialVersionUID = -9199550431465978781L;

				@Override
				public Void call(JavaPairRDD<String, String> rdd)throws Exception {
						for (OffsetRange o : offsetRanges.get()) {
							logger.info(o);
							saveOffsetToMongo(o);
									
							logger.info(o.topic() + " " + o.partition()
									+ " " + o.fromOffset() + " " + o.untilOffset());
									 
						}
						return null;
				}

				});
		
		/*directKafkaStream.transformToPair(
				new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {

				private static final long serialVersionUID = -4434360508075409004L;

				@Override
				public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd)
									throws Exception {
						OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
						offsetRanges.set(offsets);
						return rdd;
					}
				}).foreachRDD(
				new Function<JavaPairRDD<String, String>, Void>() {

					private static final long serialVersionUID = -9199550431465978781L;

				@Override
				public Void call(JavaPairRDD<String, String> rdd)throws Exception {
						for (OffsetRange o : offsetRanges.get()) {
							logger.info(o);
							saveOffsetToMongo(o);
									
							logger.info(o.topic() + " " + o.partition()
									+ " " + o.fromOffset() + " " + o.untilOffset());
									 
						}
						return null;
				}

				});*/
	}

	/**
	 * Save Kafka Offset in Mongo
	 * 
	 * @param o
	 */
	private static void saveOffsetToMongo(OffsetRange o) {
		try {
		//final DBCollection collection = db.getCollection(Config.OFFSET_COLLECTION);
		final DBCollection collection=MongoUtil.getCollection(Config.DBNAME, Config.OFFSET_COLLECTION);
		final DBObject qObj=new BasicDBObject("topic", o.topic()).append("partition", o.partition());
		final DBObject updateObj = new BasicDBObject("$set",new BasicDBObject()
						.append("fromOffset", o.fromOffset())
						.append("untilOffset", o.untilOffset())
						.append("description", o.toString()));
		
		/*final DBObject object = new BasicDBObject("topic", o.topic())
					.append("partition", o.partition())
					.append("fromOffset",o.fromOffset())
					.append("untilOffset",o.untilOffset())
					.append("description", o.toString());*/
		Thread thread=new Thread(new Runnable() {
			
			@Override
			public void run() {
				//collection.insert(object);
				collection.update(qObj, updateObj, true, false);
			}
		});
		
		thread.start();
		} catch (Exception e) {
			logger.debug("Mongo Error" + e.getMessage());
		}
	}
	
	/**
	 * Setup Spark Stream reading from Kafka.
	 * 
	 * @return
	 */
	private static JavaInputDStream<String> setupSpark() {
		SparkConf conf = new SparkConf().setAppName("Spark Aggregation")
				.setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", SparkProperties.EXECUTOR_MEMORY);
		//conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		StreamingContext ssc = new StreamingContext(conf, new Duration(25000/*Config.FIVE_MINUTE_IN_MILLISEC*/));
		jssc = new JavaStreamingContext(ssc);

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list",KafkaProperties.LOCAL_BROKER_LIST);

		// defaults to "largest"
		//kafkaParams.put("auto.offset.reset", SparkProperties.AUTO_OFFSET_RESET_EARLIEST);

		/*Set<String> topicsSet = new HashSet<>();
		topicsSet.add(KafkaProperties.KAFKA_TOPIC_NAME);*/

		Map<TopicAndPartition, Long> map=new HashMap<>();
		map.put(new TopicAndPartition(KafkaProperties.KAFKA_TOPIC_NAME, 0), 25L);
		map.put(new TopicAndPartition(KafkaProperties.KAFKA_TOPIC_NAME, 1), 5L);
		
		//MessageAndMetadata<String, String> arg8=new MessageAndMetadata<>("iBeat", partition, rawMessage, offset, StringDecoder.class, StringDecoder.class);
		/*return KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class,String.class, kafkaParams,
				map, new Function<MessageAndMetadata<String, String>, String>() {

					private static final long serialVersionUID = 5634434565638417704L;

					@Override
					public String call(MessageAndMetadata<String, String> tuple)
							throws Exception {
						logger.info("Key : " + tuple.key());
						logger.info("Msg : " + tuple.message());
						logger.info("Offset : " + tuple.offset());
						logger.info("TopicPart : " + tuple.partition() + " : " + tuple.topic());
						logger.info(tuple.productArity() + " : " + tuple.productPrefix());
						logger.info("Key Decoder : " + tuple.keyDecoder());
						return tuple.message();
					}

				} );*/
		
		KafkaManager kafkaManager=new KafkaManager(kafkaParams);
		return kafkaManager.getKafkaDirectStream(jssc,KafkaProperties.KAFKA_TOPIC_NAME);

	}

	/**
	 * Method to save Spark Stream to MongoDB
	 * 
	 * @param bsonCount
	 */
	private static void saveToDB(JavaPairDStream<Object, BSONObject> bsonCount) {
			
			final Configuration outputConfig = new Configuration();
			outputConfig.set("mongo.output.uri", SparkProperties.LOCAL_MONGO_OUTPUT_URI);
			outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
			
			bsonCount.foreachRDD(new Function<JavaPairRDD<Object,BSONObject>, Void>() {
				
				private static final long serialVersionUID = -5447455949365193895L;
	
				@Override
				public Void call(final JavaPairRDD<Object, BSONObject> rdd) throws Exception {
					//TODO Write Concern 0{un-ack}
					/*rdd.saveAsNewAPIHadoopFile("file:///notapplicable",
				            Object.class, BSONObject.class, MongoOutputFormat.class, outputConfig);*/
					
					Thread thread=new Thread(new Runnable() {
						
						@Override
						public void run() {
							rdd.saveAsNewAPIHadoopFile("file:///notapplicable",
						            Object.class, BSONObject.class, MongoOutputFormat.class, outputConfig);
						}
					});
					
					thread.start();
					return null;
				}
			});
		}

}
