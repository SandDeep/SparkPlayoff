package com.poc.ibeat.script;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
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

import java.io.IOException;
import java.util.*;

/**
 * Created by Vibhav.Rohilla on 8/18/2015.
 */
public class AggregateDashboardDataKafka {

    private static final Logger logger = Logger.getLogger(AggregateDashboardDataKafka.class);

    private final static int referer = 1;
    private final static int response = 2;
    private final static int location = 3;

    private static void executeAggregateScript(String appName, String master, long durationMillis, List<String> inputList) {

        SparkConf conf = new SparkConf().setAppName(appName)
                .setMaster(master).set("spark.executor.memory", "1g");

        StreamingContext ssc = new StreamingContext(conf, new Duration(durationMillis));
        JavaStreamingContext jssc = new JavaStreamingContext(ssc);

        Map<String, String> kafkaParams = prepareKafkaParams();

        Set<String> topicsSet = prepareTopicSet();

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
                .createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet);

        /*
        * Raw data here
        * Creates Map of each document in kafka
        * */
        JavaDStream<Map<String, Object>> lines = directKafkaStream
                .map(new Function<Tuple2<String, String>, Map<String, Object>>() {

                    private static final long serialVersionUID = 5196132687044875421L;

                    @Override
                    public Map<String, Object> call(Tuple2<String, String> tuple)
                            throws Exception {
/*                        System.out.println("====>>Direct Kafka tuple 1- " + tuple._1);
                        System.out.println("====>>Direct Kafka tuple 2- " + tuple._2);
                        System.out.println("====>>Direct Kafka Current Index- " + ++index);*/
                        return convertToMap(tuple._2);
                    }
                });

        /*
        * Pair function fetches artifactId and host, creates a new String and returns a tuple as  - artifactId_host : 1
        * for every raw data, entry like above is created.
        * There can be similar data also. eg - art1_host1:1 , art1_host1:1 in the new JavaPairDStream.
        * Now we pair these similar values using a reduceByKey function.
        * for two similar pairs, one pair is returned with count 2
        * */
        JavaPairDStream<String, Integer> viewCount = lines.mapToPair(new PairFunction<Map<String, Object>, String, Integer>() {

            private static final long serialVersionUID = 5196132687044875422L;

            @Override
            public Tuple2<String, Integer> call(Map<String, Object> map) throws Exception {
                String host = (String) map.get("host");
                String geoLocation = map.get("geoLocation").toString();
                String isIndia = map.get("isIndia").toString();
                String referrer = (String) map.get("referer");
                String group = map.get("group").toString();
                String articleId = (String) map.get("articleId");

                if (Util.isEmpty(host) || Util.isEmpty(geoLocation) || Util.isEmpty(isIndia) || Util.isEmpty(referrer) || Util.isEmpty(group) || Util.isEmpty(articleId)) {
                    logger.error("*********** Error Doc ************\n" + map);
                }
                String key = getFormattedString(host, geoLocation, isIndia, referrer, group, articleId);

                return new Tuple2<String, Integer>(key, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 7028129084583344108L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairDStream<Object, BSONObject> bsonCount = viewCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Object, BSONObject>() {

            private static final long serialVersionUID = -3964716493263543805L;

            @Override
            public Tuple2<Object, BSONObject> call(Tuple2<String, Integer> tuple)
                    throws Exception {
                DBObject dbObject = null;
                try {
                    System.out.println("Before Split tuple - " + tuple._1);
                    String[] arr = tuple._1.split("_");
                    dbObject = BasicDBObjectBuilder.start()
                            .add(arr[0], arr[1]) //host
                            .add(arr[2], arr[3]) //geoLocation
                            .add(arr[4], arr[5]) //isIndia
                            .add(arr[6], arr[7]) //referrer
                            .add(arr[8], arr[9]) //group
                            .add(arr[10], arr[11]) //article Id
                            .add("count", tuple._2).get();
                    //logger.info(dbObject);
                } catch (Exception e) {
                    logger.equals(tuple + "\n" + e.getMessage());
                }
                // null key means an ObjectId will be generated on insert
                return new Tuple2<Object, BSONObject>(null, dbObject);
            }
        });

        /*
        * Pushing to DB the refer counts
        *
        * */

        bsonCount.persist();
//        bsonCount.cache();

        for (String st : inputList) {
            int interval = Integer.parseInt(st);

            switch (interval) {
                case referer:
                    bsonCount.foreachRDD(new Function<JavaPairRDD<Object, BSONObject>, Void>() {
                        private static final long serialVersionUID = -5447455949365193895L;

                        @Override
                        public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
                            rdd.foreach(new VoidFunction<Tuple2<Object, BSONObject>>() {

                            	private static final long serialVersionUID = -1044524398675184597L;

								@Override
                                public void call(Tuple2<Object, BSONObject> objectBSONObjectTuple2) throws Exception {
/*
                                    System.out.println("====>> Retrieving touple_1 BsonCount" + objectBSONObjectTuple2._1());
                                    System.out.println("====>> Retrieving touple_2 BsonCount" + objectBSONObjectTuple2._2());*/
                                    DashboardDataMongoHandler.groupByReferrer(objectBSONObjectTuple2._2().toMap());
                                }
                            });
                            return null;
                        }
                    });
                    break;
                case response:
                    bsonCount.foreachRDD(new Function<JavaPairRDD<Object, BSONObject>, Void>() {

                        private static final long serialVersionUID = -5447455949365193895L;

                        @Override
                        public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {

                            rdd.foreach(new VoidFunction<Tuple2<Object, BSONObject>>() {
								private static final long serialVersionUID = 2075953176971680629L;

								@Override
                                public void call(Tuple2<Object, BSONObject> objectBSONObjectTuple2) throws Exception {
/*
                                    System.out.println("====>> Retrieving touple_1 BsonCountResponse" + objectBSONObjectTuple2._1());
                                    System.out.println("====>> Retrieving touple_2 BsonCountResponse" + objectBSONObjectTuple2._2());*/
                                    DashboardDataMongoHandler.groupByResponse(objectBSONObjectTuple2._2().toMap());
                                }
                            });
                            return null;
                        }
                    });
                    break;
                case location:
                    bsonCount.foreachRDD(new Function<JavaPairRDD<Object, BSONObject>, Void>() {

                        private static final long serialVersionUID = -5447455949365193895L;

                        @Override
                        public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
                            rdd.foreach(new VoidFunction<Tuple2<Object, BSONObject>>() {
								private static final long serialVersionUID = 538678553404448451L;

								@Override
                                public void call(Tuple2<Object, BSONObject> objectBSONObjectTuple2) throws Exception {
/*                                    System.out.println("====>> Retrieving touple_1 BsonCountLocation" + objectBSONObjectTuple2._1());
                                    System.out.println("====>> Retrieving touple_2 BsonCountLocation" + objectBSONObjectTuple2._2());*/
                                    DashboardDataMongoHandler.groupByLocation(objectBSONObjectTuple2._2().toMap());
                                }
                            });

                            System.out.println("======>> RDD Eelement count :" + rdd.count());
                            return null;
                        }
                    });
                    break;
                default:
                    System.exit(0);
                    break;
            }
        }
        jssc.start();
        jssc.awaitTermination();
    }

    /***
     * String at index 0 denotes spark application name.
     * String at index 1 denotes spark master
     * From index 2, args denote intervals (referrer/ response/ location)
     * @param
     */
    public static void main(String[] args) {
        args = new String[3];

        args[0] = "Spark Demo";
        args[1] = "local[2]";
        args[2] = 1 + "";

        if (invalidExecution(args)) {
            System.exit(0);
        }
        String appName = args[0];
        String master = args[1];
        long interval = 20000;
        List<String> aggregateInterval = Arrays.asList(args).subList(2,args.length);

        executeAggregateScript(appName,master,interval,aggregateInterval);
    }

    private static Set<String> prepareTopicSet() {
        Set<String> topicsSet = new HashSet<>();
        topicsSet.add("ibeat");
        return topicsSet;
    }

    private static Map<String, String> prepareKafkaParams() {
        Map<String, String> kafkaParams = new HashMap<String, String>();
        String brokers = "127.0.0.1:9092,127.0.0.1:9093";
        kafkaParams.put("metadata.broker.list", brokers);

        //defaults to "largest"
        kafkaParams.put("auto.offset.reset", "smallest");
        return kafkaParams;
    }

    private static boolean invalidExecution(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: Application name, master and Aggregate [interval(1|2)]");
            return true;
        }
        return false;
    }

    private static Map<String, Object> convertToMap(String msg) {
        ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> map;

        try {
            map = mapper.readValue(msg, new TypeReference<HashMap<String, Object>>() {
            });
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }
        return map;
    }

    private static String getFormattedString(String host, String geoLocation, String isIndia, String referrer, String group, String articleId) {
        StringBuilder key = new StringBuilder();
        key.append("host_").append(host);
        key.append("_geoLocation_").append(geoLocation);
        key.append("_isIndia_").append(isIndia);
        key.append("_referer_").append(referrer);
        key.append("_group_").append(group);
        key.append("_articleId_").append(articleId);
        return key.toString();
    }
}
