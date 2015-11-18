package com.poc.spark;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.hadoop.MongoOutputFormat;
import com.poc.ibeat.script.Config;
import com.poc.ibeat.util.DateTimeUtils;
import com.poc.ibeat.util.ScriptUtil;
import com.poc.util.SparkProperties;

/**
 * Created by Vibhav.Rohilla on 8/24/2015.
 */
public class AggregateTagsSortStream implements Serializable {
	private static final long serialVersionUID = -7758685093908927034L;
	private static MongoClient mongo;
    private final MongoClient mongoHistory;
    private static DB db;
    private static DB processDB;
    private static String dbName;
    private static final Logger logger = Logger.getLogger(AggregateTagsSortStream.class);
    private static JavaStreamingContext streamingContext;

    /**
     * Constructor to initialize DBName and  Mongo Client
     *
     * @throws UnknownHostException
     * @throws MongoException
     */
    public AggregateTagsSortStream(int interval) throws UnknownHostException, MongoException {

        dbName = Config.DBNAME + /*DateTimeUtils.getDateYYYYMMDD(getDBSuffix(interval))*/"20150428";

        mongo = new MongoClient(Config.HOST_SCRIPT,
                MongoClientOptions.builder().connectionsPerHost(10)
                        .threadsAllowedToBlockForConnectionMultiplier(15)
                        .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                        .build());

        mongoHistory = new MongoClient(Config.HOST_HISTORICAL,
                MongoClientOptions.builder().connectionsPerHost(10)
                        .threadsAllowedToBlockForConnectionMultiplier(15)
                        .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                        .build());

        db = mongo.getDB(dbName);
        processDB = mongoHistory.getDB(Config.DBNAME_PROCESS_DB);

        System.out.println("Connection to mongo initalized");
    }

    /**
     * Main Method to start execution of script
     *
     * @param args - 1st parameter specifies the interval (minute, hour etc)
     * @throws UnknownHostException
     */
    public static void main(String[] args) throws UnknownHostException {
        args = new String[1];
        args[0] = "2";
        if (invalidExecution(args)) {
            System.exit(0);
        }
        String interval = args[0];

        executeAggregateTagsSortScript(Integer.parseInt(interval));
    }

    private long getDBSuffix(int interval) {
        switch (interval) {
            case Config.MINUTE:
                long startTs = DateTimeUtils.get5MinuteSliceStamp(5);
                // startTs = 1358641800000L;

                // to set the time to the  mean  value of Start and End Time
                return (startTs - Config.MINUTE_IN_MILLISEC);
            case Config.HOUR:
                startTs = DateTimeUtils.getHourSliceStamp();
                // startTs = 1358641800000L;
                // to set the time to the mean value of Start and End Time
                return (startTs - Config.HOUR_IN_MILLISEC / 2);
            case Config.WEEK:
                startTs = DateTimeUtils.getWeeklySliceStamp();
                return (startTs - Config.DAY_IN_MILLISEC / 2);
            case Config.MONTH:
                startTs = DateTimeUtils.getMonthlySliceStamp();
                return (startTs - Config.DAY_IN_MILLISEC / 2);
            default:
                System.exit(0);
                return 0;
        }
    }


    /**
     * Method provides the duration after which the script will be re executed.
     *
     * @param interval - Specifying script type (5 min, hour etc)
     * @return
     */
    private static Duration getScriptDuration(int interval) {
        switch (interval) {
            case Config.MINUTE:
                return new Duration(20000);
            case Config.HOUR:
                return new Duration(/*DateTimeUtils.getHourSliceStamp()*/Config.HOUR_IN_MILLISEC);
            /*case Config.DAY :
            return new Duration(DateTimeUtils.getDailySliceStamp());
			*/
            case Config.WEEK:
                return new Duration(DateTimeUtils.getWeeklySliceStamp());
            case Config.MONTH:
                return new Duration(DateTimeUtils.getMonthlySliceStamp());
            default:
                System.exit(0);
                return null;
        }
    }

    /**
     * Method provides the timeStamp that will be put to DB.
     *
     * @param interval
     * @return
     */
    private static long getStartTime(int interval) {
        switch (interval) {
            case Config.MINUTE:
                return /*new Duration(20000)*/DateTimeUtils.get5MinuteSliceStamp(5);
            case Config.HOUR:
                return DateTimeUtils.getHourSliceStamp();
            /*case Config.DAY :
            return new Duration(DateTimeUtils.getDailySliceStamp());
			*/
            case Config.WEEK:
                return DateTimeUtils.getWeeklySliceStamp();
            case Config.MONTH:
                return DateTimeUtils.getMonthlySliceStamp();
            default:
                System.exit(0);
                return 0L;
        }
    }

    /**
     * Validator Method. It checks -
     * 1. Minimum one Command line argument is required.
     *
     * @param args
     * @return
     */
    private static boolean invalidExecution(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: Aggregate [interval(1|2)]");
            return true;
        }
        return false;
    }

    /**
     * Script executor
     *
     * @param interval
     */
    private static void executeAggregateTagsSortScript(int interval) throws UnknownHostException {
        AggregateTagsSortStream instance = new AggregateTagsSortStream(interval);
        handleInterval(interval);
    }

    /**
     * Method to setup following spark streaming properties -
     * 1. script Duration
     * 2. Kafka parameters to spark context.
     *
     * @param interval
     * @return JavaPairInputDStream - this is the Stream that will have raw Data for - PageTrendLog.
     */
    private static JavaPairInputDStream<String, String> setupSpark(int interval) {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", SparkProperties.EXECUTOR_MEMORY);

        /***
         * getScriptDuration() returns the batch interval.
         */
        StreamingContext ssc = new StreamingContext(conf, getScriptDuration(interval));
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
     * Executes the process of data handling on basis of interval.
     *
     * @param numericInterval
     */
    private static void handleInterval(int numericInterval) {

        switch (numericInterval) {
            case Config.MINUTE:
                JavaPairInputDStream<String, String> directKafkaStream = setupSpark(numericInterval);
                groupAndSaveForMin(directKafkaStream, numericInterval);
                break;
            case Config.HOUR:
//                groupAndSaveForHour();
                groupAndSaveForHourReadMongoDirect();
                /*startTs = DateTimeUtils.getHourSliceStamp();
                // startTs = 1358641800000L;
                // to set the time to the mean value of Start and End Time
                instance = new AggregateTagsSort(startTs - Config.HOUR_IN_MILLISEC / 2);

                System.out.println((startTs - Config.HOUR_IN_MILLISEC) + " : " + startTs);
                System.out.println(DateTimeUtils.getFormattedData(startTs)
                        + " : "
                        + DateTimeUtils.getFormattedData(startTs - 3600000));
                mro = instance.groupByTimeHour(Config.HOUR_IN_MILLISEC, startTs);
                instance.loadAggregateNew(mro, interval, startTs);*/
                //instance.storeStats(startTs, (int)interval );
                break;
            /*case Config.DAY :
                Long currentDay = DateTimeUtils.getDailySliceStamp();
				startTs = currentDay - Config.DAY_IN_MILLISEC;
				instance = new AggregateTagsSort(currentDay - Config.DAY_IN_MILLISEC/2); // to set the time to the mean value of Start and End Time
				System.out.println(startTs + " : " + DateTimeUtils.getFormattedData(startTs));
				mro = instance.groupByTimeDay(Config.DAY_IN_MILLISEC,startTs);
				instance.loadAggregateWeekMonth(mro,startTs);
				break;*/
            case Config.WEEK:
             /*   // startTs is current week Sunday & we need result from last
                // week Sunday to sat'day
                startTs = DateTimeUtils.getWeeklySliceStamp();
                System.out.println("WEEK AGGREGATION");
                System.out.println(startTs + " : " + DateTimeUtils.getFormattedData(startTs));
                instance = new AggregateTagsSort(startTs - Config.DAY_IN_MILLISEC / 2); // to get the DB of Saturday
                instance.groupByTimeWeek(startTs, 4);*/
                break;
            case Config.MONTH:
               /* // startTs is 1 day of current month & we need result from last
                // Month 1 to last
                startTs = DateTimeUtils.getMonthlySliceStamp();
                System.out.println("MONTH AGGREGATION");
                instance = new AggregateTagsSort(startTs
                        - Config.DAY_IN_MILLISEC / 2); // to set the time to the
                // mean value of Start
                // and
                // End Time
                System.out.println(startTs + " : "
                        + DateTimeUtils.getFormattedData(startTs));
                instance.groupByTimeMonth(startTs, 1);*/
                break;
            default:
              /*  instance = new AggregateTagsSort(startTs - 150000);*/
                System.exit(0);
                break;
        }
    }

    /**
     * Hourly Script.
     * Process -
     * 1. Create Java Spark context
     * 2. Create Mongo configuration
     * 3. Create a Query Object.
     * 4. Fetch data from Mongo-DB into Java RDD
     * 5. Create New PairDStream with key as host_[hostvalue]_tag_[tag value] and value as DBObject fetched.
     * 6. Apply reduceByKey operation and
     * i. merge articleId list.
     * ii. merge count.
     * 7. Push to Mongo.
     */
    private static void groupAndSaveForHourReadMongoDirect() {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", /*SparkProperties.EXECUTOR_MEMORY*/"2g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + Config.DBNAME_PROCESS_DB + "." + Config.HOURLY_TAG_COUNT);
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");

        while(true) {
        System.out.println("again starting thread...........");
        final Long startTs = DateTimeUtils.getHourSliceStamp();

        final Long refStartTm = new Long(Calendar.getInstance().getTimeInMillis());
        System.out.println("Start Time -" + refStartTm);

        DBCollection tagsCollection = db.getCollection(Config.HOURLY_TAGS_COLLECTION);
        DBCollection hourlyTagsCountCollection = processDB.getCollection(Config.HOURLY_TAG_COUNT);
        DBObject qObject = new BasicDBObject().append("timeStamp", BasicDBObjectBuilder.start("$gte", (startTs - Config.HOUR_IN_MILLISEC)).add("$lt", startTs).get());
//        DBObject qObject = new BasicDBObject().append("timeStamp", BasicDBObjectBuilder.start("$gte", (1430245800000L - 3600000L)).add("$lt", 1430245800000L).get());
        DBCursor cursor = tagsCollection.find(qObject)/*.limit(130000)*/;

        List dataList = new ArrayList<>();
        int count = 0;
        while (cursor.hasNext()) {
            dataList.add(cursor.next());
            count++;
        }

        Long refEndTm = new Long(Calendar.getInstance().getTimeInMillis());
        System.out.println("Read End Time -" + refEndTm);
        System.out.println("Read Execution Time -" + (refEndTm - refStartTm));
        System.out.println("Records parsed-" + count);

        JavaRDD<DBObject> dataRDD = javaSparkContext.parallelize(dataList);

        JavaPairRDD<String, Map<String, Object>> pairRDD = dataRDD.mapToPair(new PairFunction<DBObject, String, Map<String, Object>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 5895037445052552953L;

			@Override
            public Tuple2<String, Map<String, Object>> call(DBObject dbObject) throws Exception {
                StringBuilder key = new StringBuilder("host_").append(dbObject.get("host")).append("_tag_").append(dbObject.get("tags"));
//                System.out.println("Key =" + key.toString() + "\nData =" + dbObject);
                return new Tuple2<String, Map<String, Object>>(key.toString(), dbObject.toMap());
            }
        });
/*
        pairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {
            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("data incoming -"+stringMapTuple2._2());
            }
        });
*/


        pairRDD = pairRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
			private static final long serialVersionUID = 640895864799623368L;

			@Override
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");
                BasicDBList mergedArticles = mergeDBListsForHour((BasicDBList) map1.get("articleId"), (BasicDBList) map2.get("articleId"));

                map1.put("count", count1 + count2);
                map1.put("articleId", mergedArticles);
                return map1;
            }
        });


        JavaPairRDD<Object, BSONObject> dbData =
                pairRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, Object, BSONObject>() {
                                     
					private static final long serialVersionUID = -7136134053977523983L;

									@Override
                                      public Tuple2<Object, BSONObject> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                                          DBObject dbObject = null;
                                          String host = (String) stringMapTuple2._2().get("host");
                                          String[] arr = stringMapTuple2._1().split("_");

                                          String tags = arr[3];
                                          if (tags == null) {
                                              tags = "";
                                          }
                                          long count = (long) stringMapTuple2._2().get("count");
                                          long catIdHash = (long) (stringMapTuple2._2().get("catIdHash") == null ? 0L : stringMapTuple2._2().get("catIdHash"));
                                          String cat = (String) stringMapTuple2._2().get("cat");

                                          Integer cTagValue = null;
                                          if (tags != null && !tags.isEmpty()) {
                                              cTagValue = tags.hashCode();
                                          }
                                          DBCollection tagsCollectionTarget = db.getCollection("targetHourCOllection");
                                          BasicDBObject basicDBObject = new BasicDBObject("articleId", stringMapTuple2._2().get("articleId"));
                                          basicDBObject.append("host", host);
                                          basicDBObject.append("count", count);
                                          basicDBObject.append("cat", cat);
                                          basicDBObject.append("catIds", stringMapTuple2._2().get("catIds"));
                                          basicDBObject.append("timeStamp", startTs / 1000);
                                          basicDBObject.append("tags", tags);
                                          basicDBObject.append("cTag", cTagValue);
                                          basicDBObject.append("catIdHash", catIdHash);
//                tagsCollectionTarget.insert(dbObject);
//                stringMapTuple2._2().saveAsNewAPIHadoopFile("file:///notapplicable", Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf)
                                          return new Tuple2<Object, BSONObject>(null, dbObject);
                                      }
                                  }
                );
        /*
        * Bulk write operation
        * */
//        List<Tuple2<Object, BSONObject>> dbDataList = dbData.collect();
//        BulkWriteOperation  b1 = hourlyTagsCountCollection.initializeUnorderedBulkOperation();
//        List<DBObject> finalizedData = new ArrayList<>();
//        for(Tuple2<Object,BSONObject> tuple:dbDataList){
//            b1.insert((DBObject) tuple._2());
//        }
//        b1.execute();

        /***
         * Push every RDD to the DB
         */
//            BulkWriteResult  r1 = b1.execute();
//

        dbData.saveAsNewAPIHadoopFile("file:///notapplicable", Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
        try {
            Thread.sleep(Config.HOUR_IN_MILLISEC);
        } catch (InterruptedException e) {
            logger.error("Hourly Script Thread Interrupt");
            System.exit(0);
            return;
        }
    }

}


    /**
     * Hourly Script.
     * Process -
     * 1. Create Java Spark context
     * 2. Create Mongo configuration
     * 3. Create a Query Object.
     * 4. Fetch data from Mongo-DB into Java RDD
     * 5. Create New PairDStream with key as host_[hostvalue]_tag_[tag value] and value as DBObject fetched.
     * 6. Apply reduceByKey operation and
     * i. merge articleId list.
     * ii. merge count.
     * 7. Push to Mongo.
     */
    private static void groupAndSaveForHour() {
        final Long startTs = DateTimeUtils.getHourSliceStamp();
        ;

        JavaSparkContext sc = new JavaSparkContext("local", "Hour Script");
        Configuration config = new Configuration();
//        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/ibeat20150428." + Config.HOURLY_TAGS_COLLECTION);
//        config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/ibeat20150428." + Config.HOURLY_TAGS_COLLECTION);
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/test." + Config.HOURLY_TAGS_COLLECTION + "_2");
        config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/test." + Config.HOURLY_TAGS_COLLECTION + "_2");
        config.set("mongo.input.query", getQueryStringForHourScript());
        config.set("mongo.input.fields", getFieldsStringFroHourScript());
        config.set("mongo.input.limit", "5L");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);

        mongoRDD.foreach(new VoidFunction<Tuple2<Object, BSONObject>>() {
            private static final long serialVersionUID = -5447455949365193895L;

            @Override
            public void call(Tuple2<Object, BSONObject> tuple_2) throws Exception {
                System.out.println("tuple_2 - " + tuple_2._2());
            }
        });

        JavaPairRDD<String, Map<String, Object>> keyDataRDD = mongoRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>, String, Map<String, Object>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 4325256202326772725L;

			@Override
            public Tuple2<String, Map<String, Object>> call(Tuple2<Object, BSONObject> tuple) throws Exception {

                StringBuilder key = new StringBuilder("host_").append(tuple._2().get("host")).append("_tag_").append(tuple._2().get("tags"));
                System.out.println("Key =" + key.toString() + "\nData =" + tuple._2());
                return new Tuple2<String, Map<String, Object>>(key.toString(), tuple._2().toMap());
            }
        });


        keyDataRDD = keyDataRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 4489262377991576887L;

			@Override
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");
                BasicDBList mergedArticles = mergeDBListsForHour((BasicDBList) map1.get("articleId"), (BasicDBList) map2.get("articleId"));

                map1.put("count", count1 + count2);
                map1.put("articleId", mergedArticles);
                return map1;
            }
        });

        keyDataRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 4084216691617237813L;

			@Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                logger.info("red Data" + stringMapTuple2._2());
            }
        });

        JavaPairRDD<Object, BSONObject> dbData = keyDataRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, Object, BSONObject>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = -7396244409088113798L;

			@Override
            public Tuple2<Object, BSONObject> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                DBObject dbObject;
                String host = (String) stringMapTuple2._2().get("host");
                String[] arr = stringMapTuple2._1().split("_");

                String tags = arr[3];
                if (tags == null) {
                    tags = "";
                }
                long count = (long) stringMapTuple2._2().get("count");
                long catIdHash = (long) (stringMapTuple2._2().get("catIdHash") == null ? 0L : stringMapTuple2._2().get("catIdHash"));
                String cat = (String) stringMapTuple2._2().get("cat");
                /*BasicDBList articleIdDBList = new BasicDBList();
                Map<String, Long> articleIdMap = (Map<String, Long>) stringMapTuple2._2().get("articleId");
                if (articleIdMap != null || articleIdMap.isEmpty()) {
                    BasicDBObject articleIdAndCount;
                    for (String articleId : articleIdMap.keySet()) {
                        articleIdAndCount = new BasicDBObject();
                        articleIdAndCount.put("articleId", articleId);
                        articleIdAndCount.put("count", articleIdMap.get(articleId));
                        articleIdDBList.add(articleIdAndCount);
                    }
                }*/
                Integer cTagValue = null;
                if (!tags.isEmpty()) {
                    cTagValue = tags.hashCode();
                }

                dbObject = new BasicDBObject("articleId", stringMapTuple2._2().get("articleId"))
                        .append("host", host)
                        .append("count", count)
                        .append("cat", cat)
                        .append("catIds", stringMapTuple2._2().get("catIds"))
                        .append("timeStamp", startTs / 1000)
                        .append("tags", tags)
                        .append("cTag", cTagValue)
                        .append("catIdHash", catIdHash);

                return new Tuple2<Object, BSONObject>(null, dbObject);
            }
        });


        Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/test." + Config.HOURLY_TAG_COUNT);
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");

        /***
         * Push every RDD to the DB
         */
        dbData.saveAsNewAPIHadoopFile("file:///notapplicable", Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);

//        DBCollection hourCollection = db.getCollection(Config.HOURLY_TAGS_COLLECTION);
    }

    private static String getFieldsStringFroHourScript() {
        DBObject fieldObject = new BasicDBObject();
        fieldObject.put("host", 1);
        fieldObject.put("tags", 1);
        fieldObject.put("articleId", 1);
        fieldObject.put("count", 1);
        fieldObject.put("cat", 1);
        fieldObject.put("catIds", 1);
        fieldObject.put("cTag", 1);
        fieldObject.put("catIdHash", 1);
        return fieldObject.toString();
    }

    private static BasicDBList mergeDBListsForHour(BasicDBList list1, BasicDBList list2) {
        BasicDBList list = new BasicDBList();
        BasicDBObject obj;
        Map map2 = createMap(list2);
        for (int l = 0; l < list1.size(); l++) {
            obj = (BasicDBObject) list1.get(l);
            if (map2.get(obj.get("articleId")) != null) {
                obj.put("count", (Long) map2.get(obj.get("articleId")) + (Long) obj.get("count"));

            } else {
                map2.put(obj.get("articleId"), obj.get("count"));
            }
            list.add(obj);
        }
        return list;
    }

    private static Map createMap(BasicDBList l) {
        Map map = new HashMap<>();
        BasicDBObject obj;
        for (int i = 0; i < l.size(); i++) {
            obj = new BasicDBObject((BasicDBObject) l.get(i));
            map.put(obj.get("articleId"), obj.get("count"));
        }
        return map;
    }


    /**
     * Method returns the query object with the filter params needed to fetch 1 hour Data.
     *
     * @return String - QueryObject
     */
    private static String getQueryStringForHourScript() {
//        long currentTime = DateTimeUtils.getHourSliceStamp();
        long currentTime = Calendar.getInstance().getTimeInMillis(); //test Data
//        long startTime = currentTime - Config.HOUR_IN_MILLISEC;
        long startTime = 0L;//test Data

        DBObject qObject = new BasicDBObject().append("timeStamp", BasicDBObjectBuilder.start("$gte", startTime).add("$lt", currentTime).get());
        return qObject.toString();
    }

    /**
     * 5 minute Script.
     * Procedure -
     * 1. create Data map from Raw data.
     * 2. create a Pair DStream of key (articleId_host_tags) and value (Data from step 1)
     * 3. reduce the DStream obtained from step 2 and merge the "count" and put to the value map.
     * 4. A new Transformation is applied to DStream obtained above in which FlatMapToPair function
     * is applied to create a new Pair DStream in which all tags are flattened and
     * new key will be [individual tag]_host and value will be data map.
     * 5. DStream obtained above is reduced on key and articleId List present in Data map is merged.
     * 6. Above DStream is transformed to obtain a DStream of Mongo DB objects
     * 7. Hadoop API is used to push data to DB.
     *
     * @param directKafkaStream - Raw Data DStream
     * @param interval
     */
    private static void groupAndSaveForMin(JavaPairInputDStream<String, String> directKafkaStream, final int interval) {
        final Long startTs = DateTimeUtils.get5MinuteSliceStamp(5);

        /***
         * Creating a data map from Raw Data
         */
        JavaDStream<Map<String, Object>> inputDataMap = directKafkaStream.map(new Function<Tuple2<String, String>, Map<String, Object>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 3263111242258666268L;

			@Override
            public Map<String, Object> call(Tuple2<String, String> tuple2) throws Exception {
                Map<String, Object> dataMap = ScriptUtil.convertToMap(tuple2._2(), logger);
                System.out.println("tuple 2 for mapping - " + tuple2._2());
                Map<String, Object> map = new HashMap<>();

                try {
                    String articleId = (String) dataMap.get("articleId");
                    //Long count        	= ((Double)(Double.parseDouble((dataMap.get("count")).toString()))).longValue();
                    String cat = (String) dataMap.get("cat");
                    String host = (String) dataMap.get("host");
                    Long isAPP = 0l;
                    try {
                        isAPP = ((Double) (Double.parseDouble((dataMap.get("isAPP")).toString()))).longValue();
                    } catch (Exception e) {
                    }
                    Long contentType = 1l;
                    try {
                        contentType = ((Double) (Double.parseDouble((dataMap.get("contentType")).toString()))).longValue();
                    } catch (Exception e) {
                    }
                    Boolean isGroup = false;
                    try {
                        isGroup = Boolean.parseBoolean(dataMap.get("isGroup").toString());
                    } catch (Exception e) {
                    }

                    String url = (String) ((dataMap.get("url")));
                    Long catIdHash = (Long) (dataMap.get("catIdHash"));

                    long publishTime = 0L;
                    if (((dataMap.get("publishTime"))) != null) {
                        publishTime = ((Double) Double.parseDouble(dataMap.get("publishTime").toString())).longValue();
                    }
                    String author = "";
                    if (((dataMap.get("author"))) != null) {
                        author = ((dataMap.get("author"))).toString();
                    }
                    String channel = "";
                    if (((dataMap.get("channel"))) != null) {
                        channel = ((dataMap.get("channel"))).toString();
                    }

                    List<String> tagsList = (List<String>) map.get("tags");
                    //List<String> catsList = (List<String>) map.get("catIds");
                    List<String> subsList = (List<String>) map.get("subcat");

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
                    map.put("timeStamp", getStartTime(interval));
                    map.put("tags", tagsList);
                    map.put("catIds", dataMap.get("catIds")/* catsList */);
                    map.put("isGroup", isGroup);
                    map.put("catIdHash", catIdHash);


                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    //System.out.println(Config.getStackTrace(e));
                }

                return dataMap;

            }
        });

        /***
         * GroupLevel1 Groups - articleId, host and tags
         */
        JavaPairDStream<String, Map<String, Object>> groupLevel1 = inputDataMap

                .mapToPair(
                        new PairFunction<Map<String, Object>, String, Map<String, Object>>() {

                            private static final long serialVersionUID = 5196132687044875422L;

                            @Override
                            public Tuple2<String, Map<String, Object>> call(
                                    Map<String, Object> map) throws Exception {
                                String host = (String) map.get("host");
                                String articleId = (String) map.get("articleId");
                                List tags = (List) map.get("tags");

                                if (host == null || articleId == null) {
                                    logger.error("*********** Error Doc ************\n" + map);
                                }
                                String key = "articleId_" + articleId + "_host_" + host + "_tags_" + tags.toString();
//                                logger.info(key);
//                                System.out.println("Printing Key - " + key);
                                map.put("articlecount", 1L);

                                return new Tuple2<String, Map<String, Object>>(key, map);
                            }
                        })
                        /***
                         * Reducing on Key and adding a list of articleId along with the count.
                         */
                .reduceByKey(
                        new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public Map<String, Object> call(
                                    Map<String, Object> map1,
                                    Map<String, Object> map2) throws Exception {
                                Long count1 = (Long) map1.get("articlecount");
                                Long count2 = (Long) map2.get("articlecount");
                                Map articleIdMap = (map1.get("mappedArticles") == null) ? new HashMap() : (Map) map1.get("mappedArticles");

                                String articleId1 = (String) map1.get("articleId");
                                if (articleId1 != null || articleId1.length() > 0) {
                                    articleIdMap.put(articleId1, count1 + count2);
                                }
                                map1.put("mappedArticles", articleIdMap);
                                map1.put("articlecount", count1 + count2);
                                return map1;
                            }
                        });

        /***
         * Grouping level 1 groups on articleId+host+tags
         * Tags can be multiple for an article.
         * Grouping level 2 does -
         *  1. For each tag in a row, find occurrence of that tag in other rows.
         *  2. If one tag found in another row, then add the articleCount of current and new row and put as articleCount for that tag.
         *  Note -
         *      Idea behind this grouping is to get all article counts that contain a particular tag and preserve this value.
         */

        JavaPairDStream<String, Map<String, Object>> groupLevel2 = groupLevel1.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Map<String, Object>>, String, Map<String, Object>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = -7836531191626955406L;

			@Override
            public Iterable<Tuple2<String, Map<String, Object>>> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("group level 2 tuple 1 -" + stringMapTuple2._1());
                System.out.println("group level 2 tuple 2 -" + stringMapTuple2._2());
                ArrayList<String> tagList = (ArrayList<String>) stringMapTuple2._2().get("tags");
                ArrayList tagKeyList = new ArrayList();
                String host = (String) stringMapTuple2._2().get("host");
                StringBuilder key;
                for (String tag : tagList) {
                    key = new StringBuilder("host_").append(host).append("_tag_").append(tag);
//                    System.out.println("generated Key - " + key + "\n inserted Data -" + stringMapTuple2._2());
//                    stringMapTuple2._2().put("host-tag-value", key);
                    tagKeyList.add(new Tuple2<String, Map<String, Object>>(key.toString(), stringMapTuple2._2()));
                }
                return tagKeyList;
            }
        });

        /***
         * Structure to keep articleIDs along with their count - Map<String,Map<String,String>>
         *     Here Outer map is the Data map, Key would be "articleId", and value will be a map  of articleID->count
         */
        groupLevel2 = groupLevel2.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 3493285625747672767L;

			@Override
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("articlecount");
                Long count2 = (Long) map2.get("articlecount");
                map1.put("count", count1 + count2);
                Map articleMap1 = (Map) map1.get("mappedArticles");
                Map articleMap2 = (Map) map2.get("mappedArticles");
//                System.out.println("map of 1 -" + articleMap1);
//                System.out.println("map of 2 -" + articleMap2);
                Map map3 = mergeMapsForMin(articleMap1, articleMap2);
                map1.put("articleId", map3);
                return map1;
            }
        });

        /***
         * Printing the reduced Data on console.
         * This code section is to be removed before deployment.
         */
        //todo  - remove this printing before deployment on live.
        groupLevel2.foreach(new Function<JavaPairRDD<String, Map<String, Object>>, Void>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = -3181791588867404123L;

			@Override
            public Void call(JavaPairRDD<String, Map<String, Object>> stringMapJavaPairRDD) throws Exception {
                stringMapJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {
                                                 /**
					 * 
					 */
					private static final long serialVersionUID = -6007447245849231526L;

												@Override
                                                 public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                                                     System.out.println("tuple 1 after reduce 2 - " + stringMapTuple2._1());
                                                     System.out.println("tuple 2 after reduce 2 - " + stringMapTuple2._2());
                                                 }
                                             }
                );
                return null;
            }
        });

        /***
         * Create a Stream of BSon objects that are to be pushed to mongo.
         */
        JavaPairDStream<Object, BSONObject> dbData = groupLevel2.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, Object, BSONObject>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = -710445074235453189L;

			@Override
            public Tuple2<Object, BSONObject> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                DBObject dbObject = null;
                String host = (String) stringMapTuple2._2().get("host");
                String[] arr = stringMapTuple2._1().split("_");

                String tags = arr[3];
                if (tags == null) {
                    tags = "";
                }
                long count = (long) stringMapTuple2._2().get("count");
                long catIdHash = (long) (stringMapTuple2._2().get("catIdHash") == null ? 0L : stringMapTuple2._2().get("catIdHash"));
                String cat = (String) stringMapTuple2._2().get("cat");
                BasicDBList articleIdDBList = new BasicDBList();
                Map<String, Long> articleIdMap = (Map<String, Long>) stringMapTuple2._2().get("articleId");
                if (articleIdMap != null || articleIdMap.isEmpty()) {
                    BasicDBObject articleIdAndCount;
                    for (String articleId : articleIdMap.keySet()) {
                        articleIdAndCount = new BasicDBObject();
                        articleIdAndCount.put("articleId", articleId);
                        articleIdAndCount.put("count", articleIdMap.get(articleId));
                        articleIdDBList.add(articleIdAndCount);
                    }
                }
                Integer cTagValue = null;
                if (tags != null && !tags.isEmpty()) {
                    cTagValue = tags.hashCode();
                }

                dbObject = new BasicDBObject("articleId", articleIdDBList)
                        .append("host", host)
                        .append("count", count)
                        .append("cat", cat)
                        .append("catIds", stringMapTuple2._2().get("catIds"))
                        .append("timeStamp", startTs)
                                //.append("interval", interval)
                        .append("tags", tags)
                        .append("cTag", cTagValue)
                        .append("catIdHash", catIdHash);

                return new Tuple2<Object, BSONObject>(null, dbObject);
            }
        });
        /***
         * Printing the BSON objects
         */
        //todo  - remove the printing before deployment on live.
        dbData.foreach(new Function<JavaPairRDD<Object, BSONObject>, Void>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = -7328536733418370292L;

			@Override
            public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
                rdd.foreach(new VoidFunction<Tuple2<Object, BSONObject>>() {
                    /**
					 * 
					 */
					private static final long serialVersionUID = 1821652879534980134L;

					@Override
                    public void call(Tuple2<Object, BSONObject> tuple) throws Exception {
                        System.out.println("created DB Object -" + tuple._2());
                    }
                });
                return null;
            }
        });
        /***
         * Preparing MongoDB Configuration object
         */
        final Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/test" + ".tagsAggrtCount_2");
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");

        /***
         * Push every RDD to the DB
         */
        dbData.foreachRDD(new Function<JavaPairRDD<Object, BSONObject>, Void>() {
            private static final long serialVersionUID = -5447455949365193895L;

            @Override
            public Void call(JavaPairRDD<Object, BSONObject> rdd) throws Exception {
                //rdd.saveAsTextFile("E://spark.log");
                rdd.saveAsNewAPIHadoopFile("file:///notapplicable",
                        Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
                return null;
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    /**
     * Preparing Topic set for Kafka
     *
     * @return topic set
     */
    private static Set<String> prepareTopicSet() {
        Set topicsSet = new HashSet<>();
        topicsSet.add("ibeat");
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
        String brokers = "127.0.0.1:9092,127.0.0.1:9093";
        kafkaParams.put("metadata.broker.list", brokers);

        //defaults to "largest"
        kafkaParams.put("auto.offset.reset", "smallest");
        return kafkaParams;
    }

    /**
     * Method to merge the articleIds map for the 5 minute script.
     *
     * @param map1
     * @param map2
     * @return
     */
    private static Map mergeMapsForMin(Map map1, Map<String, Object> map2) {
        if (map1 == null) {
            return map2;
        }
        if (map2 == null) {
            return map1;
        }
        Map mergedMap = new HashMap<>(map1);
        for (String articleId : map2.keySet()) {
            long count1 = (long) map2.get(articleId);
            if (mergedMap.containsKey(articleId)) {
                long count2 = (long) mergedMap.get(articleId);
                mergedMap.put(articleId, (count1 + count2));
                System.out.println("articleId -" + articleId + " Count1 fetched -" + count1 + " and Count2 fetched -" + count2);
            } else {
                mergedMap.put(articleId, count1);
                System.out.println("articleId -" + articleId + " Count1 fetched -" + count1);
            }
        }
        return mergedMap;
    }

}
