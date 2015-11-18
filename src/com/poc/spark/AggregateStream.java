package com.poc.spark;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.bson.BSONObject;

import scala.Tuple2;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.hadoop.MongoOutputFormat;
import com.poc.ibeat.util.DateTimeUtils;
import com.poc.util.Config;
import com.poc.util.Constants;
import com.poc.util.HelperUtil;
import com.poc.util.KafkaProperties;
import com.poc.util.SparkProperties;

/**
 * Direct Approach (No Receivers) - Use Kafka�s simple consumer API to Read data
 *
 * @author Deepesh.Maheshwari
 */
public class AggregateStream {

    private static final Logger logger = Logger.getLogger(AggregateStream.class);
    private static JavaStreamingContext jssc;
    private static final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
    private static MongoClient mongo, mongoHistory;
    private static DB db;

    /**
     * Main method to execute script.
     * Remove args before deploying on prod
     * @param args
     */
    public static void main(String[] args) {
        setupMongo();

        args = new String[1];
        args[0] = "1";


        if (invalidExecution(args)) {
            System.exit(0);
        }


        int interval = Integer.parseInt(args[0]);

        JavaPairInputDStream<String, String> directKafkaStream = setupSpark();
        
        switch (interval) {
            case Config.MINUTE:
                JavaPairDStream<Object, BSONObject> bsonCount = groupByMin(directKafkaStream);
                saveToDB(bsonCount);

                break;
            case Config.HOUR:
                groupAndSaveByHour();
                
                break;
            case Config.DAY:
                Long currentDay = DateTimeUtils.getDailySliceStamp();
                long startTm = currentDay - Config.DAY_IN_MILLISEC;
                List<DBObject> dailyDataList = getDBDataForDailyScript(startTm);
                groupAndSaveByDay(dailyDataList, currentDay, startTm);

                break;
            default:
                break;
        }
        
        jssc.start();
        jssc.awaitTermination();

    }

    /***
     * Group and save data on daily basis
     * @param dailyDataList - DB List containing a single day's data
     * @param currentDay - Current Day in Millis
     * @param startTm - Start time of query. (last Day of month in millis)
     */
    private static void groupAndSaveByDay(List<DBObject> dailyDataList, final long currentDay, long startTm) {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties./*LOCAL_*/SPARK_MASTER)
                .set("spark.executor.memory", SparkProperties.EXECUTOR_MEMORY);
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        

        JavaRDD<DBObject> dailyDataRDD = javaSparkContext.parallelize(dailyDataList);

/*        JavaPairRDD<String, Map<String, Object>> pairRDD = dataRDD.mapToPair(new PairFunction<DBObject, String, Map<String, Object>>() {
            @Override
            public Tuple2<String, Map<String, Object>> call(DBObject dbObject) throws Exception {
                StringBuilder key = new StringBuilder("host_").append(dbObject.get("host")).append("_cat_").append(dbObject.get("tags"));
//                System.out.println("Key =" + key.toString() + "\nData =" + dbObject);
                return new Tuple2<String, Map<String, Object>>(key.toString(), dbObject.toMap());
            }
        });*/

        /***
         * Make key value pair from the data fetched from DB.
         * Key created here -
         *  1. host_isAPP_articleId
         */

        JavaPairRDD<String, Map<String, Object>> dailyPairRDD = dailyDataRDD.mapToPair(new PairFunction<DBObject, String, Map<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            @SuppressWarnings("unchecked")
            public Tuple2<String, Map<String, Object>> call(DBObject object) throws Exception {
                StringBuilder keyArticleAppHost = new StringBuilder("articleId_").append(object.get("articleId")).
                        append("_isAPP_").append(object.get("isAPP")).
                        append("_host_").append(object.get("host"));

                object.put("timeStamp", currentDay / 1000);
                Set<String> tagSet = new HashSet<String>();

                if ((object.get("tags")) != null) {
                    Set<String> keys = ((BasicDBList) (object.get("tags"))).keySet();

                    for (String key : keys) {
                        String tid = (((BasicDBList) (object.get("tags"))).get(key)).toString();
                        if (tid != null && !tid.isEmpty()) {
                            tagSet.add(tid);
                        }
                    }
                }
                object.put("tags", tagSet);

                return (new Tuple2<String, Map<String, Object>>(keyArticleAppHost.toString(), object.toMap()));
            }
        });


/*
        refPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("Key Created =" + stringMapTuple2._1());
                System.out.println("Map Created =" + stringMapTuple2._2());
            }
        });
*/

        /***
         * Reduce the javaPairRDD on key created above
         */
        dailyPairRDD = dailyPairRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

            private static final long serialVersionUID = -4240713101290240751L;

            @Override
            @SuppressWarnings("unchecked")
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");
                HashSet<String> tags1 = (HashSet<String>) map1.get("tags");
                HashSet<String> tags2 = (HashSet<String>) map1.get("tags");
                tags1.addAll(tags2);
                map1.put("tags", tags1);
                map1.put("count", count1 + count2);
                return map1;
            }
        });


/*
        dailyPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("key Reduced=" + stringMapTuple2._1());
                System.out.println("Map Reduced=" + stringMapTuple2._2());
            }
        });
*/

        /***
         * Create new JavaPair rdd as - (null,object) to ensure system generated _id is used in mongo.
         */
        dailyPairRDD = dailyPairRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, String, Map<String, Object>>() {

            private static final long serialVersionUID = 4980477777146438979L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                storeMonthWeekCount(new BasicDBObject(stringMapTuple2._2()));
                long t = DateTimeUtils.getNDaysbackDailySliceStamp(1);
                long time = t / 1000l;
                stringMapTuple2._2().remove("timeStamp");
                stringMapTuple2._2().put("ts", time);
                return new Tuple2<String, Map<String, Object>>(null, stringMapTuple2._2());
            }
        });

 /*       refPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("Key =" + stringMapTuple2._1());
                System.out.println("Value =" + stringMapTuple2._2());
            }
        });
*/
        /***
         * Preparing MongoDB Configuration object
         */


        final Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + com.poc.ibeat.script.Config.DBNAME_PROCESS_DB + "_test" + "." + com.poc.ibeat.script.Config.DAILY_ARTICLE_COUNT + "__dgvefrgvs");
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        dailyPairRDD.saveAsNewAPIHadoopFile("file:///notapplicable",
                Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
        javaSparkContext.close();

    }

    /***
     * Store month and week data.
     * No changes were made in this method.
     * @param dbl - Data to b
     */
    @Deprecated
    @SuppressWarnings("unused")
    private static void storeMonthWeekCount(List<DBObject> dbl) {
        String monthDBName = com.poc.ibeat.script.Config.DBNAME_MONTH_DB;
        DB articleDBMonth = mongoHistory.getDB(monthDBName);
        DBCollection articleCollMonth = articleDBMonth.getCollection(com.poc.ibeat.script.Config.MONTH_ARTICLE_COUNT_COLLECTION);

        try {
            DBObject query = null;
            BasicDBObject bDbObj = null;
            String articleId = null;
            long t = System.currentTimeMillis();
            /*
			Long weekTime = (DateTimeUtils.getWeeklySliceStamp()/1000l);

			for (DBObject dbo : dbl)
			{
				articleId = dbo.get("articleId").toString();
				if (articleId != null && articleId != "") {
					query =	new BasicDBObject("articleId",
							dbo.get("articleId")).append("host",
							dbo.get("host")).append("isAPP",
									dbo.get("isAPP")).append("ts",
											weekTime);

					Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
					dbObjIdMap.put("catIds", new BasicDBObject("$each",dbo.get("catIds")));
					dbObjIdMap.put("tags",new BasicDBObject("$each", dbo.get("tags")));

					bDbObj = new BasicDBObject("$setOnInsert",new BasicDBObject("cat", dbo.get("cat"))
									.append("subcat", dbo.get("subcat"))
									.append("catIdHash", dbo.get("catIdHash"))
									.append("catIds", dbo.get("catIds"))
									.append("url", dbo.get("url"))
									.append("isAPP", dbo.get("isAPP"))
									.append("ts", weekTime)
									.append("contentType", dbo.get("contentType"))
									.append("isGroup", dbo.get("isGroup"))
									.append("publishTime",dbo.get("publishTime"))
									.append("author", dbo.get("author"))
									.append("channel", dbo.get("channel")))
									.append("$addToSet",dbObjIdMap)
									.append("$inc", new BasicDBObject("count",((Double)Double.parseDouble(dbo.get("count").toString())).longValue()));

					articleCollWeek.update(query, bDbObj, true, false);
				}
			}
			System.out.println("Time taken to save week count data--" + (System.currentTimeMillis() -t));*/
            Long monthTime = (DateTimeUtils.getMonthlySliceStamp() / 1000l);
            int dayOfMonth = DateTimeUtils.getDayOfMonth(System.currentTimeMillis());
            if (dayOfMonth != 1) {
                monthTime = (DateTimeUtils.getMonthlySliceStamp() / 1000l);
            } else {
                Long date = System.currentTimeMillis() - Config.DAY_IN_MILLISEC;
                monthTime = (DateTimeUtils.getMonthlySliceStamp(date) / 1000l);
            }

            t = System.currentTimeMillis();
            for (DBObject dbo : dbl) {
                articleId = dbo.get("articleId").toString();
                String host = dbo.get("host").toString();
                long count = ((Double) Double.parseDouble(dbo.get("count").toString())).longValue();
                if (checkLimit(host, count)) {
                    if (articleId != null && articleId != "") {
                        query = new BasicDBObject("articleId",
                                dbo.get("articleId")).append("host",
                                dbo.get("host")).append("isAPP",
                                dbo.get("isAPP")).append("ts",
                                monthTime);

                        Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
						/*dbObjIdMap.put("catIds", new BasicDBObject("$each",dbo.get("catIds")));*/
                        dbObjIdMap.put("tags", new BasicDBObject("$each", dbo.get("tags")));

                        bDbObj = new BasicDBObject("$setOnInsert",
                                new BasicDBObject("cat", dbo.get("cat"))
                                        .append("subcat", dbo.get("subcat"))
                                        .append("catIdHash", dbo.get("catIdHash"))
                                        .append("catIds", dbo.get("catIds"))
                                        .append("url", dbo.get("url"))
                                        .append("isAPP", dbo.get("isAPP"))
                                        .append("ts", monthTime)
                                                //.append("tsd", new Date(monthTime*1000l))
                                        .append("contentType", dbo.get("contentType"))
                                        .append("isGroup", dbo.get("isGroup"))
                                        .append("publishTime", dbo.get("publishTime"))
                                        .append("author", dbo.get("author"))
                                        .append("channel", dbo.get("channel")))
                                .append("$addToSet", dbObjIdMap)
                                .append("$inc", new BasicDBObject("count"
                                        , ((Double) Double.parseDouble(dbo.get("count").toString())).longValue()));
                        articleCollMonth.update(query, bDbObj, true, false);
                    }
                }

            }
            logger.info("Time taken to save month count data--" + (System.currentTimeMillis() - t));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void storeMonthWeekCount(DBObject dbo) {
        String monthDBName = com.poc.ibeat.script.Config.DBNAME_MONTH_DB;
        DB articleDBMonth = mongoHistory.getDB(monthDBName);
        DBCollection articleCollMonth = articleDBMonth.getCollection(com.poc.ibeat.script.Config.MONTH_ARTICLE_COUNT_COLLECTION);

        try {
            DBObject query = null;
            BasicDBObject bDbObj = null;
            String articleId = null;
            long t = System.currentTimeMillis();

            Long monthTime = (DateTimeUtils.getMonthlySliceStamp() / 1000l);
            int dayOfMonth = DateTimeUtils.getDayOfMonth(System.currentTimeMillis());
            if (dayOfMonth != 1) {
                monthTime = (DateTimeUtils.getMonthlySliceStamp() / 1000l);
            } else {
                Long date = System.currentTimeMillis() - Config.DAY_IN_MILLISEC;
                monthTime = (DateTimeUtils.getMonthlySliceStamp(date) / 1000l);
            }

            t = System.currentTimeMillis();
                articleId = dbo.get("articleId").toString();
                String host = dbo.get("host").toString();
                long count = ((Double) Double.parseDouble(dbo.get("count").toString())).longValue();
                if (checkLimit(host, count)) {
                    if (articleId != null && articleId != "") {
                        query = new BasicDBObject("articleId",
                                dbo.get("articleId")).append("host",
                                dbo.get("host")).append("isAPP",
                                dbo.get("isAPP")).append("ts",
                                monthTime);

                        Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
						/*dbObjIdMap.put("catIds", new BasicDBObject("$each",dbo.get("catIds")));*/
                        dbObjIdMap.put("tags", new BasicDBObject("$each", dbo.get("tags")));

                        bDbObj = new BasicDBObject("$setOnInsert",
                                new BasicDBObject("cat", dbo.get("cat"))
                                        .append("subcat", dbo.get("subcat"))
                                        .append("catIdHash", dbo.get("catIdHash"))
                                        .append("catIds", dbo.get("catIds"))
                                        .append("url", dbo.get("url"))
                                        .append("isAPP", dbo.get("isAPP"))
                                        .append("ts", monthTime)
                                                //.append("tsd", new Date(monthTime*1000l))
                                        .append("contentType", dbo.get("contentType"))
                                        .append("isGroup", dbo.get("isGroup"))
                                        .append("publishTime", dbo.get("publishTime"))
                                        .append("author", dbo.get("author"))
                                        .append("channel", dbo.get("channel")))
                                .append("$addToSet", dbObjIdMap)
                                .append("$inc", new BasicDBObject("count"
                                        , ((Double) Double.parseDouble(dbo.get("count").toString())).longValue()));
                        articleCollMonth.update(query, bDbObj, true, false);
                    }
                }

            logger.info("Time taken to save month count data--" + (System.currentTimeMillis() - t));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean checkLimit(String host, long count) {

        if (Arrays.asList(Constants.bigHostList).contains(host) && count < 2) {
            return false;
        } else {
            return true;
        }
    }


    private static List<DBObject> getDBDataForDailyScript(long startTm) {
        DBObject fields = new BasicDBObject("articleId", 1);
        fields.put("host", 1);
        fields.put("isAPP", 1);
        fields.put("cat", "$cat");
        fields.put("subcat", "$subcat");
        fields.put("publishTime", "$publishTime");
        fields.put("channel", "$channel");
        fields.put("author", "$author");
        fields.put("tags", 1);
        fields.put("url", "$url");
        fields.put("catIds", 1);
        fields.put("catIdHash", 1);
        fields.put("count", 1);
        fields.put("contentType", 1);
        fields.put("isGroup", 1);
        fields.put("_id", 0);

        DB historyDb = mongoHistory.getDB(com.poc.ibeat.script.Config.DBNAME_PROCESS_DB);

        int countLimit = Constants.OTHERHOST_DAY_LIMIT;

        DBCollection collHourWiseArticles = historyDb.getCollection(com.poc.ibeat.script.Config.HOURLY_ARTICLE_COUNT);


        DBObject qObject =new BasicDBObject("host",new BasicDBObject("$in",Arrays.asList(Constants.hostList)))
                .append("count",BasicDBObjectBuilder.start("$gte", countLimit).get())
                .append("timeStamp", BasicDBObjectBuilder.start("$gt", startTm / 1000)
                        .add("$lte", (DateTimeUtils.getDailySliceStamp()) / 1000).get());
/*
        DBObject qObject = new BasicDBObject("host", new BasicDBObject("$in", Arrays.asList(Constants.hostList)))
                .append("timeStamp", 1441200600);
*/

        DBCursor cursor = collHourWiseArticles.find(qObject);

        List<DBObject> referDataList = new ArrayList<>();

        long count = 0;
        while (cursor.hasNext()) {
            referDataList.add(cursor.next());
            count++;
        }
        logger.info("Records read daily script=" + count);
        return referDataList;
    }

    private static boolean invalidExecution(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: Aggregate [interval(1|2)]");
            return true;
        }
        return false;
    }

    private static void setupMongo() {
        try {
            mongo = new MongoClient(Config.LOCAL_DB_SERVER_ADDRESS, MongoClientOptions
                    .builder().connectionsPerHost(10)
                    .threadsAllowedToBlockForConnectionMultiplier(15)
                    .connectTimeout(5000).writeConcern(WriteConcern.NORMAL).build());

            mongoHistory = new MongoClient(Config.HOST_HISTORICAL/*"localhost"*/,
                    MongoClientOptions.builder().connectionsPerHost(10)
                            .threadsAllowedToBlockForConnectionMultiplier(15)
                            .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                            .build());

            //mongo = MongoClientSingleton.getInstance();
            db = mongo.getDB(Config.DBNAME);
        } catch (UnknownHostException e) {
			logger.debug("Mongo Setup Error" + e.getMessage());
        }
    }

    private static JavaPairDStream<Object, BSONObject> groupByMin(
            JavaPairInputDStream<String, String> directKafkaStream) {

        //Update Kafka Offset
        saveKafkaOffset(directKafkaStream);

        final Long startTs = DateTimeUtils.get5MinuteSliceStamp(5);

        JavaPairDStream<String, StreamObject> vc = directKafkaStream.mapToPair(new PairFunction<Tuple2<String, String>, String, StreamObject>() {

            private static final long serialVersionUID = -556686240923635777L;

            @SuppressWarnings("unchecked")
            @Override
            public Tuple2<String, StreamObject> call(Tuple2<String, String> tuple)
                    throws Exception {
                logger.debug(tuple._2());

                Map<String, Object> object = HelperUtil.convertToMap(tuple._2());
                Map<String, Object> map = new HashMap<>();
                StreamObject streamObject = new StreamObject();
                String key = "";

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
                        key = "articleId_" + articleId + "_host_" + host + "_isAPP_" + isAPP;

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
                Set<String> tags1 = s1.getTags();
                Set<String> tags2 = s2.getTags();

                tags1.addAll(tags2);

                s1.setTags(tags1);
                s1.setCount(count1 + count2);

                return s1;
            }
        });

        vc.print(20);
        //viewCount.print(100);

        return vc.mapToPair(new PairFunction<Tuple2<String, StreamObject>, Object, BSONObject>() {

            private static final long serialVersionUID = 6144855830933465260L;

            @Override
            public Tuple2<Object, BSONObject> call(
                    Tuple2<String, StreamObject> tuple) throws Exception {
                DBObject dbObject = null;
                try {

                    StreamObject sObj = tuple._2();
                    Map<String, Object> map = sObj.getMap();
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
        /*return viewCount
                .mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, Object, BSONObject>() {

					private static final long serialVersionUID = -3964716493263543805L;

					@Override
					public Tuple2<Object, BSONObject> call(
							Tuple2<String, Map<String, Object>> tuple)
							throws Exception {
						//logger.debug(tuple);
						DBObject dbObject = null;
						try {

							dbObject = new BasicDBObject(tuple._2());

							//logger.info(dbObject);
						} catch (Exception e) {
							logger.(tuple + "\n" + e.getMessage());
						}

						// null key means an ObjectId will be generated on insert
						return new Tuple2<Object, BSONObject>(null, dbObject);
					}
				});*/

        //return bsonMongo;
    }

    /**
     * Process and Save kafka Offset
     *
     * @param directKafkaStream
     */
    private static void saveKafkaOffset(JavaPairInputDStream<String, String> directKafkaStream) {

        directKafkaStream.transformToPair(
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
                    public Void call(JavaPairRDD<String, String> rdd) throws Exception {
                        for (OffsetRange o : offsetRanges.get()) {
                            logger.info(o);
                            saveOffsetToMongo(o);

                            logger.info(o.topic() + " " + o.partition()
                                    + " " + o.fromOffset() + " " + o.untilOffset());

                        }
                        return null;
                    }

                });
    }

    /**
     * Save Kafka Offset in Mongo
     *
     * @param o
     */
    private static void saveOffsetToMongo(OffsetRange o) {
        try {
            final DBCollection collection = db.getCollection(Config.OFFSET_COLLECTION);
            
            final DBObject qObj=new BasicDBObject("topic", o.topic()).append("partition", o.partition());
			final DBObject updateObj = new BasicDBObject("$set",new BasicDBObject()
							.append("fromOffset", o.fromOffset())
							.append("untilOffset", o.untilOffset())
							.append("description", o.toString()));
            
			/*final DBObject object = new BasicDBObject("topic", o.topic())
                    .append("partition", o.partition())
                    .append("fromOffset", o.fromOffset())
                    .append("untilOffset", o.untilOffset())
                    .append("description", o.toString());*/
			
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
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
    private static JavaPairInputDStream<String, String> setupSpark() {
        SparkConf conf = new SparkConf().setAppName("Spark Aggregation")
                .setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", SparkProperties.EXECUTOR_MEMORY);
        //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        StreamingContext ssc = new StreamingContext(conf, new Duration(20000/*Config.FIVE_MINUTE_IN_MILLISEC*/));
        jssc = new JavaStreamingContext(ssc);

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", KafkaProperties.LOCAL_BROKER_LIST);

        // defaults to "largest"
        //kafkaParams.put("auto.offset.reset", SparkProperties.AUTO_OFFSET_RESET_EARLIEST);

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

    /**
     * Method to save Spark Stream to MongoDB
     *
     * @param bsonCount
     */
    private static void saveToDB(JavaPairDStream<Object, BSONObject> bsonCount) {

        final Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", SparkProperties.LOCAL_MONGO_OUTPUT_URI);
        outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");

        bsonCount.foreachRDD(new Function<JavaPairRDD<Object, BSONObject>, Void>() {

            private static final long serialVersionUID = -5447455949365193895L;

            @Override
            public Void call(final JavaPairRDD<Object, BSONObject> rdd) throws Exception {
                //TODO Write Concern 0{un-ack}
                    /*rdd.saveAsNewAPIHadoopFile("file:///notapplicable",
                            Object.class, BSONObject.class, MongoOutputFormat.class, outputConfig);*/

                Thread thread = new Thread(new Runnable() {

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

    private static void groupAndSaveByHour() {

        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties./*LOCAL_*/SPARK_MASTER)
                .set("spark.executor.memory", SparkProperties.EXECUTOR_MEMORY);
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        final long curTs = DateTimeUtils.getHourSliceStamp();

        List<DBObject> referDataList = getDBDataForHourlyScript(curTs);
//        List<DBObject> referDataList = getDBDataForHourlyScript_TestData(curTs);

        JavaRDD<DBObject> referrerDataRDD = javaSparkContext.parallelize(referDataList);

/*        JavaPairRDD<String, Map<String, Object>> pairRDD = dataRDD.mapToPair(new PairFunction<DBObject, String, Map<String, Object>>() {
            @Override
            public Tuple2<String, Map<String, Object>> call(DBObject dbObject) throws Exception {
                StringBuilder key = new StringBuilder("host_").append(dbObject.get("host")).append("_cat_").append(dbObject.get("tags"));
//                System.out.println("Key =" + key.toString() + "\nData =" + dbObject);
                return new Tuple2<String, Map<String, Object>>(key.toString(), dbObject.toMap());
            }
        });*/

        /***
         * Make key value pair from the data fetched from DB.
         * Key created here -
         *  1. host_isAPP_articleId
         */

        JavaPairRDD<String, Map<String, Object>> refPairRDD = referrerDataRDD.mapToPair(new PairFunction<DBObject, String, Map<String, Object>>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            @SuppressWarnings("unchecked")
            public Tuple2<String, Map<String, Object>> call(DBObject object) throws Exception {
                StringBuilder keyArticleAppHost = new StringBuilder("articleId_").append(object.get("articleId")).
                        append("_isAPP_").append(object.get("isAPP")).
                        append("_host_").append(object.get("host"));

// Data cleaning part
/*
                String articleId = (String) (((DBObject) object.get("_id"))).get("articleId");
                Long count = ((Double) (Double.parseDouble((object.get("count")).toString()))).longValue();
                String cat = (String) (object.get("cat"));
                String host = (String) (((DBObject) object.get("_id"))).get("host");
                Long isAPP = 0l;
                try {
                    isAPP = ((Double) (Double.parseDouble((((DBObject) object.get("_id")).get("isAPP")).toString()))).longValue();
                } catch (Exception e) {
                }
                Long contentType = 1l;
                try {
                    contentType = ((Double) (Double.parseDouble((object.get("contentType")).toString()))).longValue();
                } catch (Exception e) {
                }
                Boolean isGroup = false;
                try {
                    isGroup = Boolean.parseBoolean(object.get("isGroup").toString());
                } catch (Exception e) {
                }
                String url = (String) ((object.get("url")));
                Long catIdHash = (Long) (object.get("catIdHash"));

                // Normalize Tags
                Set<String> tagsList = new HashSet<String>();

                if ((object.get("tags")) != null) {
                    Set<String> keys = ((BasicDBList) (object.get("tags"))).keySet();

                    for (String key : keys) {
                        //tagsList.add((((BasicDBList) (object.get("tags"))).get(key)).toString());
                        String tid = (((BasicDBList) (object.get("tags"))).get(key)).toString();
                        if (tid != null && !tid.isEmpty()) {
                            tagsList.add(tid);
                        }
                    }
                }

                // Normalize Sub Cat
                Set<String> subsList = new HashSet<String>();
                Object obj = object.get("subcat");
                if (obj instanceof BasicDBList) {
                    if ((obj) != null) {
                        Set<String> keys = ((BasicDBList) (obj)).keySet();
                        for (String key : keys) {
                            //subsList.add((((BasicDBList) (obj)).get(key)).toString());
                            String sid = (((BasicDBList) (obj)).get(key)).toString();
                            if (sid != null && !sid.isEmpty()) {
                                subsList.add(sid);
                            }
                        }
                    }
                } else if (obj instanceof String) {
                    subsList.add((String) obj);
                } else {
                    subsList.add((String) obj);
                }

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


                object.put("timeStamp", curTs);
                object.put("articleId", articleId);
                object.put("host", host);
                object.put("isAPP", isAPP);
                object.put("contentType", contentType);
                object.put("count", count);
                object.put("cat", cat);
                object.put("subcat", subsList);
                object.put("url", url);
                object.put("publishTime", publishTime);
                object.put("author", author);
                object.put("channel", channel);
                object.put("tags", tagsList);
                object.put("isGroup", isGroup);
                object.put("catIdHash", catIdHash);
*/
                object.put("timeStamp", curTs / 1000);
                Set<String> tagSet = new HashSet<String>();

                if ((object.get("tags")) != null) {
                    Set<String> keys = ((BasicDBList) (object.get("tags"))).keySet();

                    for (String key : keys) {
                        //tagsList.add((((BasicDBList) (object.get("tags"))).get(key)).toString());
                        String tid = (((BasicDBList) (object.get("tags"))).get(key)).toString();
                        if (tid != null && !tid.isEmpty()) {
                            tagSet.add(tid);
                        }
                    }
                }
                object.put("tags", tagSet);

                return (new Tuple2<String, Map<String, Object>>(keyArticleAppHost.toString(), object.toMap()));
            }
        });


/*
        refPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("Key Created =" + stringMapTuple2._1());
                System.out.println("Map Created =" + stringMapTuple2._2());
            }
        });
*/

        /***
         * Reduce the javaPairRDD on key created above
         */
        refPairRDD = refPairRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

            private static final long serialVersionUID = -4240713101290240751L;

            @Override
            @SuppressWarnings("unchecked")
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");
                HashSet<String> tags1 = (HashSet<String>) map1.get("tags");
                HashSet<String> tags2 = (HashSet<String>) map1.get("tags");
                tags1.addAll(tags2);
                map1.put("tags", tags1);
                map1.put("count", count1 + count2);
                return map1;
            }
        });


/*
        refPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {
            */
/**
 *
 *//*

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("key Reduced=" + stringMapTuple2._1());
                System.out.println("Map Reduced=" + stringMapTuple2._2());
            }
        });
*/

        /***
         * Create new JavaPair rdd as - (null,object) to ensure system generated _id is used in mongo.
         */
        refPairRDD = refPairRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, String, Map<String, Object>>() {

            private static final long serialVersionUID = 4980477777146438979L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                return new Tuple2<String, Map<String, Object>>(null, stringMapTuple2._2());
            }
        });

/*        refPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("Key =" + stringMapTuple2._1());
                System.out.println("Value =" + stringMapTuple2._2());
            }
        });*/

        /***
         * Preparing MongoDB Configuration object
         */
        final Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + com.poc.ibeat.script.Config.DBNAME_PROCESS_DB + "_test" + "." + com.poc.ibeat.script.Config.HOURLY_ARTICLE_COUNT);
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        refPairRDD.saveAsNewAPIHadoopFile("file:///notapplicable",
                Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
        javaSparkContext.close();
    }

    /**
     * Method to generate test data for the script
     *
     * @param curTs - time slice
     * @return
     */
    @Deprecated
    @SuppressWarnings("unused")
    private static List<DBObject> getDBDataForHourlyScript_TestData(long curTs) {
        ArrayList<DBObject> testList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            DBObject dbObject = new BasicDBObject();
            dbObject.put("_id", "553e81573848d7f2cdf943c8");
            dbObject.put("articleId", "35678709");
            dbObject.put("host", "happytrips.com" + i);
            dbObject.put("count", 6l);
            dbObject.put("cat", "Destinations");
            dbObject.put("subcat", "[ ]");
            dbObject.put("url", "http://www.happytrips.com/destinations/shimla-in-pictures/ss35678709.cms");
            dbObject.put("publishTime", 0l);
            dbObject.put("author", "");
            dbObject.put("channel", "happytrips");
            dbObject.put("timeStamp", 1441953000l);
            dbObject.put("tags", new BasicDBList());
            dbObject.put("catIds", "[\"Destinations\"]");
            dbObject.put("isAPP", 0l);
            dbObject.put("catIdHash", "-7383616549059319235");
            testList.add(dbObject);
        }
        return testList;
    }

    /**
     * Method to generate test data for the script
     *
     * @param curTs - time slice
     * @return
     */
    @Deprecated
    @SuppressWarnings("unused")
    private static List<DBObject> getDBDataForDailyScript_TestData(long curTs) {
        ArrayList<DBObject> testList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {

            DBObject dbObject = new BasicDBObject();
            dbObject.put("_id", "55e6fa593848bf220b5ceaa9");
            dbObject.put("articleId", "22969");
            dbObject.put("host", "zigwheels.com");
            dbObject.put("isAPP", 0l);
            dbObject.put("contentType", 1l);
            dbObject.put("count", 6l);
            dbObject.put("count", 6l);
            dbObject.put("cat", "Destinations");
            dbObject.put("url", "http://www.happytrips.com/destinations/shimla-in-pictures/ss35678709.cms");
            dbObject.put("publishTime", 0l);
            dbObject.put("author", "");
            dbObject.put("channel", "happytrips");
            dbObject.put("timeStamp", 1441953000l);
            dbObject.put("catIds", "[\"Destinations\"]");
            dbObject.put("isGroup", false);
            dbObject.put("catIdHash", "-7383616549059319235");
            testList.add(dbObject);
        }
        return testList;
    }

    /**
     * Method to get Data from DB for hourly script.
     * Fetched fields -
     * a. Article Id
     * b. Host
     * c. isAPP
     * d. Category
     * e. Sub category
     * f. Publish time
     * g. Channel
     * h. Author
     * i. Tags
     * j. URL
     * k. Category Ids
     * l. Category Id Hash
     * m. Count
     * n. Content Type
     * o. IsGroup
     * <p>
     * Query parameters -
     * a. Current hour slice in millis
     * b. Past hour slice in millis
     * c. Host must be present in provided host List
     *
     * @param curTs - Hour time slice
     * @return - List of DBObjects
     */
    private static List<DBObject> getDBDataForHourlyScript(long curTs) {
        String dbName = Config.DBNAME /*"ibeat"*/ + DateTimeUtils.getDateYYYYMMDD(curTs - Config.HOUR_IN_MILLISEC / 2)/*"20150428"*/;
        db = mongo.getDB(dbName);
        DBCollection collTimedCount = db.getCollection(Config.HOURLY_SOURCE_COLLECTION);
        DBObject fields = new BasicDBObject("articleId", 1);
        fields.put("host", 1);
        fields.put("isAPP", 1);
        fields.put("cat", "$cat");
        fields.put("subcat", "$subcat");
        fields.put("publishTime", "$publishTime");
        fields.put("channel", "$channel");
        fields.put("author", "$author");
        fields.put("tags", 1);
        fields.put("url", "$url");
        fields.put("catIds", 1);
        fields.put("catIdHash", 1);
        fields.put("count", 1);
        fields.put("contentType", 1);
        fields.put("isGroup", 1);
        fields.put("_id", 0);
        long startTs = curTs - Config.HOUR_IN_MILLISEC;
        DBObject qObject = new BasicDBObject("host", new BasicDBObject("$in", Arrays.asList(Constants.hostList)))
                .append("timeStamp", BasicDBObjectBuilder.start("$gt", startTs)
                        .add("$lte", curTs).get());
/*
        DBObject qObject = new BasicDBObject("host", new BasicDBObject("$in", Arrays.asList(Constants.hostList)))
                .append("timeStamp", 1441564500000l);
*/

        DBCursor cursor = collTimedCount.find(qObject);

        List<DBObject> referDataList = new ArrayList<>();

        long count = 0;
        while (cursor.hasNext()) {
            referDataList.add(cursor.next());
            count++;
        }
        System.out.println("Records read =" + count);
        return referDataList;
    }


}
