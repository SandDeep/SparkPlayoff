package com.poc.spark;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import org.apache.hadoop.conf.Configuration;
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

import scala.Tuple2;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.hadoop.MongoOutputFormat;
import com.poc.ibeat.script.Config;
import com.poc.mongo.MongoRead;
import com.poc.util.DateTimeUtils;
import com.poc.util.HelperUtil;
import com.poc.util.KafkaProperties;
import com.poc.util.SparkProperties;

/**
 * File created By - Vibhav
 */
public class UserAgentCollectionStream {
    private static final Logger logger = Logger.getLogger(AggregateTagsSortStream.class);
    private static JavaStreamingContext streamingContext;
    private static MongoClient mongo, mongoArticle;
    private static final DB db, dbArticle;
    private static final DBCollection collHostDeviceMap, collArticle, collHostDevice;
    private static HashMap<String, BasicDBObject> userMap;
    private static final Long time = DateTimeUtils.getHourSliceStamp() / 1000;

    static {
        try {
            mongo = new MongoClient(/*Config.HOST_SCRIPT*/"localhost",
                    MongoClientOptions.builder().connectionsPerHost(10)
                            .threadsAllowedToBlockForConnectionMultiplier(15)
                            .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                            .build());
            mongoArticle = new MongoClient(/*Config.HOST_SCRIPT*/"localhost",
                    MongoClientOptions.builder().connectionsPerHost(10)
                            .threadsAllowedToBlockForConnectionMultiplier(15)
                            .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                            .build());
        } catch (UnknownHostException e) {
            mongo = null;
            mongoArticle = null;
            System.exit(0);
        }
        long startTs = DateTimeUtils.getHourSliceStamp();

        String dbName = Config.DBNAME + DateTimeUtils.getDateYYYYMMDD(startTs - Config.HOUR_IN_MILLISEC / 2);
        db = mongo.getDB(dbName);
        collHostDeviceMap = db.getCollection(Config.HOST_DEVICE_COLLECTION);
        DB dashboardDB = mongo.getDB(Config.DBNAME_DASHBOARD_DB);
        collHostDevice = dashboardDB.getCollection(Config.HOST_DEVICE_AGG_COLLECTION);

        dbArticle = mongoArticle.getDB(Config.DBNAME_ARTICLE_DB);
        collArticle = dbArticle.getCollection(Config.USER_AGENT_COLLECTION);
        userMap = (HashMap<String, BasicDBObject>)
                MongoRead.getAllData(Config.USER_AGENT_COLLECTION, Config.DBNAME_ARTICLE_DB, "UserAgent", "Detail");
    }

    /**
     * Grouped on  - host, agentType
     */
    private static void updateUserAgentData() {

        JavaPairInputDStream<String, String> directKafkaStream = setupSpark();
        JavaDStream<Map<String, Object>> inputDataMap = directKafkaStream.map(new Function<Tuple2<String, String>, Map<String, Object>>() {

            private static final long serialVersionUID = 2697658494500913915L;

            @Override
            public Map<String, Object> call(Tuple2<String, String> tuple2) throws Exception {
                Map<String, Object> dataMap = HelperUtil.convertToMap(tuple2._2());
                Map<String, Object> map = new HashMap<>();

                assert dataMap != null;
                Long count;
                try {
                    count = ((Double) (Double.parseDouble((dataMap.get("count")).toString()))).longValue();
                } catch (Exception e) {
                    count = 1L;
                }
                // populate DataMap

                long downTime = 0l;
                Object downloadTimeObj =  dataMap.get("pgDownloadTime");
                if(downloadTimeObj instanceof Integer){
                    downTime = ((Integer) downloadTimeObj).longValue();
                }else{
                    downTime = (long) downloadTimeObj;
                }

                map.put("agentType", dataMap.get("agentType"));
                map.put("host", dataMap.get("host"));
                map.put("count", count);
                map.put("dnldTime", downTime);
                map.put("elementNumber", 1L);
                System.out.println("Kafka Data -" + map);
                return map;

            }
        });

        /***
         * Grouping on host and agentType
         */
        JavaPairDStream<String, Map<String, Object>> groupedHostAgent = inputDataMap

                .mapToPair(
                        new PairFunction<Map<String, Object>, String, Map<String, Object>>() {

                            private static final long serialVersionUID = 5196132687044875422L;

                            @Override
                            public Tuple2<String, Map<String, Object>> call(
                                    Map<String, Object> map) throws Exception {
                                String host = (String) map.get("host");
                                String agentType = (String) map.get("agentType");
                                StringBuilder key = new StringBuilder("host_").append(host).append("_agentType_").append(agentType);
                                if (host == null || agentType == null) {
                                    logger.error("*********** Error Doc ************\n" + map);
                                }
                                map.put("count", 1L);
                                return new Tuple2<String, Map<String, Object>>(key.toString(), map);
                            }
                        })
                        /***
                         * Reducing on Key and adding a list of articleId along with the count.
                         * Putting total element count and total download time to calculate the average for hostDeviceMap collection.
                         */
                .reduceByKey(
                        new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

                            private static final long serialVersionUID = 8268957366129889642L;

                            @Override
                            public Map<String, Object> call(
                                    Map<String, Object> map1,
                                    Map<String, Object> map2) throws Exception {
                                Long count1 = (Long) map1.get("count");
                                Long count2 = (Long) map2.get("count");
                                Long elementCount1 = (Long) map1.get("elementNumber");
                                Long elementCount2 = (Long) map2.get("elementNumber");
                                Long downloadTime1 = (Long) map1.get("dnldTime");
                                Long downloadTime2 = (Long) map2.get("dnldTime");
                                map1.put("count", count1 + count2);
                                map1.put("elementNumber", elementCount1 + elementCount2);
                                System.out.println("totalElementNumber -" + map1.get("elementNumber"));
                                map1.put("dnldTime", downloadTime1 + downloadTime2);
                                System.out.println("totalDownloadTime-" + map1.get("totalDownloadTime"));
                                return map1;
                            }
                        });

        /***
         * Calculating the average downloadTime
         */


        /***
         * Preparing DStream for hostDeviceMap collection.
         * At the end, this DStream is pushed to DB using hadoop API.
         */
        JavaPairDStream<String, BSONObject> hostDeviceMapStream = groupedHostAgent.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, String, BSONObject>() {

            private static final long serialVersionUID = 6791091399063073933L;

            @Override
            public Tuple2<String, BSONObject> call(Tuple2<String, Map<String, Object>> tuple2) throws Exception {
                int isDesktop = 1;
                UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
                String agentType = (String) tuple2._2().get("agentType");
                BasicDBObject detailAgentObject = userMap.get(agentType);

                ReadableUserAgent agent;
                String deviceCat, browserName;

                long totalDownloadTime = (long) tuple2._2().get("dnldTime");
                long totalCount = (long) tuple2._2().get("elementNumber");
                long averageDownloadTime = totalDownloadTime / totalCount;


                if (detailAgentObject == null) {
                    agent = parser.parse(agentType);
                    deviceCat = agent.getDeviceCategory().getName();
                    browserName = agent.getName();
                } else {
                    deviceCat = detailAgentObject.get("DeviceCategory").toString();
                    browserName = detailAgentObject.get("BrowserName").toString();

                }

                if (deviceCat != null
                        && (deviceCat.contains("phone")
                        || deviceCat
                        .contains("mobile")
                        || deviceCat
                        .contains("Tab") || deviceCat
                        .contains("tab"))) {
                    isDesktop = 0;
                }

                DBObject insertObj = new BasicDBObject(
                        "host", tuple2._2().get("host"))
                        .append("Device", isDesktop)
                        .append("Browser", browserName)
                        .append("Count", totalCount)
                        .append("AVG", averageDownloadTime)
                        .append("time", time);
                System.out.println("New collection data -"+insertObj);
                return new Tuple2<String, BSONObject>(null, insertObj);
            }
        });

        /***
         * Parsing RDD and performing below steps -
         *  1. parse the incoming agent Details.
         *  2. Check for existing of this agent in existing database.
         *  3. If exists, increment the count of the record corresponding to host, device and browser name.
         *  4. If does not exists -
         *      a. create the entry in UserAgent collection of article DB.
         *      b. increment the count of the record corresponding to host, device and browser name.
         */
        groupedHostAgent.foreach(new Function<JavaPairRDD<String, Map<String, Object>>, Void>() {

            private static final long serialVersionUID = -7754863955426441326L;

            @Override
            public Void call(JavaPairRDD<String, Map<String, Object>> javaRDD) throws Exception {
                javaRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

                    private static final long serialVersionUID = -8186182616228361570L;

                    @Override
                    public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                        String host = (String) stringMapTuple2._2().get("host");
                        long count = (long) stringMapTuple2._2().get("count");
                        String agentType = (String) stringMapTuple2._2().get("agentType");
                        UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
                        ReadableUserAgent agent = parser.parse(agentType);

                        if (userMap == null || userMap.isEmpty()) {
                            System.out.println("User Map null");
                            String browserName = agent.getName();
                            String deviceCat = agent.getDeviceCategory()
                                    .getName();
                            int isDesktop = 1;
                            if ((deviceCat.contains("phone") || deviceCat
                                    .contains("mobile") || deviceCat
                                    .contains("Tab") || deviceCat
                                    .contains("tab"))) {
                                isDesktop = 0;
                            }

                            DBObject queryObj = new BasicDBObject("host", host).append("Device", isDesktop).append("Browser", browserName);
                            /*BasicDBObject updateObj = new BasicDBObject("$inc", new BasicDBObject("Count", count));
                            collHostDeviceMap.update(queryObj, updateObj, true, false);*/
                            updateHostDeviceCollection(queryObj, count);
                        } else {
                            BasicDBObject agentDetailsObj;
                            BasicDBObject agentDetailsUserMap = userMap.get(agentType);
                            if (agentDetailsUserMap != null) {
                                agentDetailsObj = agentDetailsUserMap;
                            } else {
                                BasicDBObject agentDetails = new BasicDBObject("BrowserName", agent.getName())
                                        .append("BrowserVersion", agent.getVersionNumber().toVersionString())
                                        .append("BrowserType", agent.getType().name())
                                        .append("DeviceCategory", agent.getDeviceCategory().getName())
                                        .append("OSName", agent.getOperatingSystem().getName())
                                        .append("OSfamily", agent.getOperatingSystem().getFamilyName());

                                DBObject newAgentDetails = new BasicDBObject("UserAgent",
                                        agentType).append("Detail", agentDetails);
                                agentDetailsObj = agentDetails;
                                System.out.println("HostDeviceData update [not null user map] - " + newAgentDetails);
                                userMap.put(agentType, agentDetails);
                                insertNewArticleAgentDetails(newAgentDetails);
                            }

                            String browserName = agentDetailsObj.get("BrowserName").toString();
                            String deviceCat = agentDetailsObj.get("DeviceCategory").toString();
                            int isDesktop = 1;
                            if ((deviceCat.contains("phone") ||
                                    deviceCat.contains("mobile")
                                    || deviceCat.contains("Tab")
                                    || deviceCat.contains("tab"))) {
                                isDesktop = 0;
                            }
                            DBObject queryObj = new BasicDBObject("host", host).append("Device", isDesktop).
                                    append("Browser", browserName);

                            /*BasicDBObject updateObj = new BasicDBObject("$inc", new BasicDBObject("Count", count));
                            collHostDeviceMap.update(queryObj, updateObj, true, false);*/
                            updateHostDeviceCollection(queryObj, count);
                        }
                    }
                });
                return null;
            }
        });

        /***
         * Preparing MongoDB Configuration object for host Device Map
         */
        final Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + Config.DBNAME_DASHBOARD_DB + "." + Config.HOST_DEVICE_AGG_COLLECTION + "myCustiom");
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        hostDeviceMapStream.foreach(new Function<JavaPairRDD<String, BSONObject>, Void>() {

            private static final long serialVersionUID = -5238252963762127001L;

            @Override
            public Void call(JavaPairRDD<String, BSONObject> v1) throws Exception {
                v1.saveAsNewAPIHadoopFile("file:///notapplicable",
                        Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
                return null;
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    /**
     * Method to setup following spark streaming properties -
     * 1. script Duration
     * 2. Kafka parameters to spark context.
     *
     * @return JavaPairInputDStream - this is the Stream that will have raw Data for - PageTrendLog.
     */
    private static JavaPairInputDStream<String, String> setupSpark() {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster("local[2]").set("spark.executor.memory", SparkProperties.EXECUTOR_MEMORY);

        StreamingContext ssc = new StreamingContext(conf, new Duration(20000/*Config.HOUR_IN_MILLISEC*/));
        streamingContext = new JavaStreamingContext(ssc);

        Map<String, String> kafkaParams = prepareKafkaParams();

        Set<String> topicsSet = prepareTopicSet();

        /***
         * Kafka Stream Data
         */
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
                .createDirectStream(
                        streamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet);
        return directKafkaStream;
    }

    /**
     * Preparing Topic set for Kafka
     *
     * @return topic set
     */
    private static Set<String> prepareTopicSet() {
        Set<String> topicsSet = new HashSet<>();
        topicsSet.add(KafkaProperties.KAFKA_TOPIC_NAME);
        return topicsSet;
    }

    /**
     * Preparing kafka parameters -
     * 1. Brokers
     * 2. Offset
     *
     * @return map of kafka parameters
     */
    private static Map<String, String> prepareKafkaParams() {
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", KafkaProperties.LOCAL_BROKER_LIST);

        //defaults to "largest"
        kafkaParams.put("auto.offset.reset", "smallest");
        return kafkaParams;
    }

    public static void main(String[] args) throws UnknownHostException {
        updateUserAgentData();
    }

    /**
     * method updates the existing record (or inserts new) with count generated after aggregation of data from Kafka.
     *
     * @param queryObject - object to be updated in DB.
     * @param count       - count to be incremented in existing record.
     */
    private static void updateHostDeviceCollection(final DBObject queryObject, final long count) {
        if (queryObject == null) {
            return;
        }

        BasicDBObject updateObj = new BasicDBObject("$inc", new BasicDBObject("Count", count));
        collHostDeviceMap.update(queryObject, updateObj, true, false);
        Thread mongoUpdate = new Thread(new Runnable() {
            @Override
            public void run() {
                BasicDBObject updateObj = new BasicDBObject("$inc", new BasicDBObject("Count", count));
                collHostDeviceMap.update(queryObject, updateObj, true, false);
            }
        });
        mongoUpdate.start();

    }

    /**
     * Inserts the new Object to Article collection
     *
     * @param newAgentDetails - DBObject of new User Agent.
     */
    private static void insertNewArticleAgentDetails(final DBObject newAgentDetails) {
        if (newAgentDetails == null) {
            return;
        }
        Thread mongoUpdate = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    collArticle.insert(newAgentDetails);
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        });
        mongoUpdate.start();
    }
}