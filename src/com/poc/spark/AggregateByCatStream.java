package com.poc.spark;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.poc.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;

import scala.Tuple2;

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
import com.poc.util.DateTimeUtils;
import com.poc.util.SparkProperties;

/**
 * Created By Vibhav
 */
public class AggregateByCatStream {
    private static final Logger logger = Logger.getLogger(AggregateTagsSortStream.class);
    private static MongoClient mongo, mongoHistory;
    private static final DB db, processDB;
    private static final String dbName;

    static {
        try {
            mongo = new MongoClient(Config.HOST_SCRIPT,
                    MongoClientOptions.builder().connectionsPerHost(10)
                            .threadsAllowedToBlockForConnectionMultiplier(15)
                            .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                            .build());

            mongoHistory = new MongoClient(Config.HOST_SCRIPT,
                    MongoClientOptions.builder().connectionsPerHost(10)
                            .threadsAllowedToBlockForConnectionMultiplier(15)
                            .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                            .build());

        } catch (UnknownHostException e) {
            logger.error(e);
            mongo = null;
            System.exit(0);
        }
        long curTs = DateTimeUtils.getHourSliceStamp();

        dbName = Config.DBNAME + DateTimeUtils.getDateYYYYMMDD(curTs - 1800000);
        db = mongo.getDB(dbName);
        processDB = mongoHistory.getDB(Config.DBNAME_PROCESS_DB);
    }

    public static void main(String[] args) throws UnknownHostException, MongoException {

        long curTs = DateTimeUtils.getHourSliceStamp();

        groupAndSaveClicks(curTs);
        /***
         *  todo Location script currently stopped. Need data to test this.
         */
        groupAndSaveLocation(curTs);

        /***
         *  todo Location script currently stopped. Need data to test this.
         */
        groupAndSaveReferrer(curTs);

        /***
         *  todo Location script currently stopped. Need data to test this.
         */
        groupAndSaveResponse(curTs);

        storeStats(curTs);
    }

    /***
     * Save time stamp for which execution on the script was done.
     * @param curTs - hour time Stamp
     */
    private static void storeStats(long curTs) {
        DBCollection coll = db.getCollection(Config.CAT_STATS_COLLECTION);
        DBObject q = new BasicDBObject("execTs", curTs);
        DBObject o = new BasicDBObject("$set",
                new BasicDBObject("execTs", curTs));
        //System.out.println(o.toString());
        coll.update(q, o, true, false);

    }

    /***
     * Method aggregates on host, category and group. Steps -
     *  1. fetch Data from dgrpCount collection of ibeat DB.
     *  2. Create JavaRDD of this Data.
     *  3. Map this data to get flattened(specific and All category) results.
     *  4. Push the Aggregated Data to responseByCat collection of iBeat DB.
     * @param curTs - hour time Stamp
     */
    private static void groupAndSaveResponse(final long curTs) {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", /*SparkProperties.EXECUTOR_MEMORY*/"2g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);


        DBCollection collResponse = db.getCollection(Config.RESPONSE_COLLECTION);
        DBObject fields = new BasicDBObject("host", 1);
        fields.put("cat", 1);
        fields.put("group", 1);
        fields.put("count", 1);
        fields.put("totTm", 1);
        fields.put("_id", 0);
        DBObject qObject = new BasicDBObject("timeStamp", curTs);

        DBCursor cursor = collResponse.find(qObject);

        List<DBObject> respDataList = new ArrayList<>();

        int count = 0;
        while (cursor.hasNext()) {
            respDataList.add(cursor.next());
            count++;
        }
        System.out.println("Records read - " + count);


        JavaRDD<DBObject> groupDataRDD = javaSparkContext.parallelize(respDataList);

/*        JavaPairRDD<String, Map<String, Object>> pairRDD = dataRDD.mapToPair(new PairFunction<DBObject, String, Map<String, Object>>() {
            @Override
            public Tuple2<String, Map<String, Object>> call(DBObject dbObject) throws Exception {
                StringBuilder key = new StringBuilder("host_").append(dbObject.get("host")).append("_cat_").append(dbObject.get("tags"));
//                System.out.println("Key =" + key.toString() + "\nData =" + dbObject);
                return new Tuple2<String, Map<String, Object>>(key.toString(), dbObject.toMap());
            }
        });*/
        /***
         * Flatten the results fetched from DB.
         * Keys created here -
         *  1. host_cat_group
         *  2. host_cat_group (cat = All)
         */
        JavaPairRDD<String, Map<String, Object>> groupPairRDD = groupDataRDD.flatMapToPair(new PairFlatMapFunction<DBObject, String, Map<String, Object>>() {

            private static final long serialVersionUID = 2153674427026304998L;

            @Override
            @SuppressWarnings("unchecked")
            public Iterable<Tuple2<String, Map<String, Object>>> call(DBObject dbObject) throws Exception {
                ArrayList<Tuple2<String, Map<String, Object>>> dataList = new ArrayList<Tuple2<String, Map<String, Object>>>();
                dbObject.put("timeStamp", curTs);
                StringBuilder keyHostCatGroup = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append(dbObject.get("cat")).
                        append("_group_").append(dbObject.get("group"));

                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatGroup.toString(), dbObject.toMap()));

                StringBuilder keyHostCatAllGroup = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append(dbObject.get("All")).
                        append("_group_").append(dbObject.get("group"));
                DBObject dbObjectCatAll = new BasicDBObject(dbObject.toMap());
                dbObjectCatAll.put("cat","All");
                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatAllGroup.toString(), dbObjectCatAll.toMap()));
                return dataList;
            }
        });

        /***
         * Reduce the javaPairRDD on key created above
         */
        groupPairRDD = groupPairRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

            private static final long serialVersionUID = 6248577433560652086L;

            @Override
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");

                Long totTm1 = (Long) map1.get("totTm");
                Long totTm2 = (Long) map2.get("totTm");
                map1.put("totTm", totTm1 + totTm2);
                map1.put("count", count1 + count2);
                return map1;
            }
        });


        /***
         * Create new JavaPair rdd as - (null,object) to ensure system generated _id is used in mongo.
         */
        groupPairRDD = groupPairRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, String, Map<String, Object>>() {

            private static final long serialVersionUID = 4980477777146438979L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                return new Tuple2<String, Map<String, Object>>(null, stringMapTuple2._2());
            }
        });
/*

        groupPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = -8662861573103155393L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("Key =" + stringMapTuple2._1());
                System.out.println("Value =" + stringMapTuple2._1());
            }
        });
*/

        /***
         * Preparing MongoDB Configuration object
         */
        final Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + dbName + "." + Config.RESP_CAT_COLLECTION);
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        groupPairRDD.saveAsNewAPIHadoopFile("file:///notapplicable",
                Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
        javaSparkContext.close();
    }


    /***
     * Method aggregates on host, category and referer. Steps -
     *  1. fetch Data from refererCount collection of ibeat DB.
     *  2. Create JavaRDD of this Data.
     *  3. Map this data to get flattened(specific and All category) results.
     *  4. Push the Aggregated Data to refererByCat collection of iBeat DB.
     * @param curTs - hour time Stamp
     */
    private static void groupAndSaveReferrer(long curTs) {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", /*SparkProperties.EXECUTOR_MEMORY*/"2g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);


        DBCollection collReferrer = db.getCollection(Config.REFERER_COLLECTION);
        DBObject fields = new BasicDBObject("host", true);
        fields.put("cat", true);
        fields.put("referer", true);
        fields.put("count", true);
        fields.put("_id", false);
        DBObject qObject = new BasicDBObject("timeStamp", curTs);

        DBCursor cursor = collReferrer.find(qObject);

        List<DBObject> referDataList = new ArrayList<>();

        int count = 0;
        while (cursor.hasNext()) {
            referDataList.add(cursor.next());
            count++;
        }
        System.out.println("Records read - " + count);


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
         * Flatten the results fetched from DB.
         * Keys created here -
         *  1. host_cat_referer
         *  2. host_cat_referer (cat = All)
         */
        JavaPairRDD<String, Map<String, Object>> refPairRDD = referrerDataRDD.flatMapToPair(new PairFlatMapFunction<DBObject, String, Map<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            @SuppressWarnings("unchecked")
            public Iterable<Tuple2<String, Map<String, Object>>> call(DBObject dbObject) throws Exception {
                ArrayList<Tuple2<String, Map<String, Object>>> dataList = new ArrayList<Tuple2<String, Map<String, Object>>>();
                StringBuilder keyHostCatRef = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append(dbObject.get("cat")).
                        append("_referer_").append(dbObject.get("referer"));

                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatRef.toString(), dbObject.toMap()));

                StringBuilder keyHostCatAllRef = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append("All").
                        append("_referer_").append(dbObject.get("referer"));
                DBObject dbObjectCatAll = new BasicDBObject(dbObject.toMap());
                dbObjectCatAll.put("cat","All");
                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatAllRef.toString(), dbObjectCatAll.toMap()));
                return dataList;
            }
        });


        /***
         * Reduce the javaPairRDD on key created above
         */
        refPairRDD = refPairRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

            private static final long serialVersionUID = -4240713101290240751L;

            @Override
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");
                map1.put("count", count1 + count2);
                return map1;
            }
        });


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
                System.out.println("Value =" + stringMapTuple2._1());
            }
        });*/

        /***
         * Preparing MongoDB Configuration object
         */
        final Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + dbName + "." + Config.REF_CAT_COLLECTION);
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        refPairRDD.saveAsNewAPIHadoopFile("file:///notapplicable",
                Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
        javaSparkContext.close();
    }



    /***
     * Method aggregates on host, category, location and isIndia flag. Steps -
     *  1. fetch Data from locationCount collection of ibeat DB.
     *  2. Create JavaRDD of this Data.
     *  3. Map this data to get flattened(specific and All category) results.
     *  4. Push the Aggregated Data to locationByCat collection of iBeat DB.
     * @param curTs - hour time Stamp
     */
    private static void groupAndSaveLocation(final long curTs) {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", /*SparkProperties.EXECUTOR_MEMORY*/"2g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);


        DBCollection collLocationCount = db.getCollection(Config.LOCATION_COLLECTION);
        DBObject fields = new BasicDBObject("host", 1);
        fields.put("cat", 1);
        fields.put("location", 1);
        fields.put("isIndia", 1);
        fields.put("count", 1);
        fields.put("_id", 0);
        DBObject qObject = new BasicDBObject("timeStamp", curTs);

        DBCursor cursor = collLocationCount.find(qObject);
        List<DBObject> locDataList = new ArrayList<>();
        int count = 0;
        while (cursor.hasNext()) {
            locDataList.add(cursor.next());
            count++;
        }
        System.out.println("Records read - " + count);


        JavaRDD<DBObject> locDataRDD = javaSparkContext.parallelize(locDataList);

/*        JavaPairRDD<String, Map<String, Object>> pairRDD = dataRDD.mapToPair(new PairFunction<DBObject, String, Map<String, Object>>() {
            @Override
            public Tuple2<String, Map<String, Object>> call(DBObject dbObject) throws Exception {
                StringBuilder key = new StringBuilder("host_").append(dbObject.get("host")).append("_cat_").append(dbObject.get("tags"));
//                System.out.println("Key =" + key.toString() + "\nData =" + dbObject);
                return new Tuple2<String, Map<String, Object>>(key.toString(), dbObject.toMap());
            }
        });*/

        /***
         * Flatten the results fetched from DB.
         * Keys created here -
         *  1. host_cat_location_isIndia
         *  2. host_cat_location_isIndia (location = All)
         *  3. host_cat_location_isIndia (cat = All)
         *  4. host_cat_location_isIndia (location = All and cat = All)
         */
        JavaPairRDD<String, Map<String, Object>> locPairRDD = locDataRDD.flatMapToPair(new PairFlatMapFunction<DBObject, String, Map<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            @SuppressWarnings("unchecked")
            public Iterable<Tuple2<String, Map<String, Object>>> call(DBObject dbObject) throws Exception {
                ArrayList<Tuple2<String, Map<String, Object>>> dataList = new ArrayList<Tuple2<String, Map<String, Object>>>();
                dbObject.put("timeStamp", curTs);

                //                start of host, cat, location and isIndia
                StringBuilder keyHostCatLocInd = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append(dbObject.get("cat")).
                        append("_location_").append(dbObject.get("location")).
                        append("_isIndia_").append(dbObject.get("isIndia"));
                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatLocInd.toString(), dbObject.toMap()));

                DBObject dbObjectLocAll = new BasicDBObject(dbObject.toMap());
                StringBuilder keyHostCatLocAllInd = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append(dbObject.get("cat")).
                        append("_location_").append("All").
                        append("_isIndia_").append(dbObject.get("isIndia"));
                dbObjectLocAll.put("location","All");
                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatLocAllInd.toString(), dbObjectLocAll.toMap()));

                //                End of host, cat, location and isIndia

                //                Start of host, cat - All, location and isIndia
                DBObject dbObjectCatAll = new BasicDBObject(dbObject.toMap());
                StringBuilder keyHostCatAllLocInd = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append("All").
                        append("_location_").append(dbObject.get("location")).
                        append("_isIndia_").append(dbObject.get("isIndia"));
                dbObjectCatAll.put("cat", "All");
                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatAllLocInd.toString(), dbObjectCatAll.toMap()));


                DBObject dbObjectCatAllLocAll = new BasicDBObject(dbObject.toMap());
                StringBuilder keyHostCatAllLocAllInd = new StringBuilder("host_").append(dbObject.get("host")).
                        append("_cat_").append("All").
                        append("_location_").append("All").
                        append("_isIndia_").append(dbObject.get("isIndia"));
                dbObjectCatAllLocAll.put("cat","All");
                dbObjectCatAllLocAll.put("location","All");
                dataList.add(new Tuple2<String, Map<String, Object>>(keyHostCatAllLocAllInd.toString(), dbObjectCatAllLocAll.toMap()));

                return dataList;
            }
        });


        /***
         * Reduce the javaPairRDD on key created above
         */
        locPairRDD = locPairRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");
                map1.put("count", count1 + count2);
                return map1;
            }
        });

        /***
         * Create new JavaPair rdd as - (null,object) to ensure system generated _id is used in mongo.
         */
        locPairRDD = locPairRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, String, Map<String, Object>>() {

            private static final long serialVersionUID = 4980477777146438979L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                return new Tuple2<String, Map<String, Object>>(null, stringMapTuple2._2());
            }
        });

    /*    locPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                System.out.println("Key =" + stringMapTuple2._1());
                System.out.println("Value =" + stringMapTuple2._1());
            }
        });*/

        /***
         * Preparing MongoDB Configuration object
         */
        final Configuration minuteScriptMongoConf = new Configuration();
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + dbName + "." + Config.LOC_CAT_COLLECTION);
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        locPairRDD.saveAsNewAPIHadoopFile("file:///notapplicable",
                Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
        javaSparkContext.close();
    }


    /***
     * Method aggregates on host, category and referer. Steps -
     *  1. fetch Data from hourWiseArticleCount collection of ibeat DB.
     *  2. Create JavaRDD of this Data.
     *  3. Map this data to get flattened(specific and All category) results.
     *  4. Push the Aggregated Data to clicksByCat collection of iBeat DB.
     * @param curTs - hour time Stamp
     */
    private static void groupAndSaveClicks(final long curTs) {
        SparkConf conf = new SparkConf().setAppName(SparkProperties.APPLICATION_NAME)
                .setMaster(SparkProperties.LOCAL_SPARK_MASTER).set("spark.executor.memory", /*SparkProperties.EXECUTOR_MEMORY*/"2g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);


        DBCollection collHrWiseArticleCount = processDB.getCollection(Config.HOURLY_ARTICLE_COUNT);
        DBObject fields = new BasicDBObject("host", true);
        fields.put("cat", true);
        fields.put("count", true);
        fields.put("_id", false);
        int countLimit = Constants.OTHERHOST_WEEK_LIMIT;
        DBObject qObject = new BasicDBObject("host", new BasicDBObject("$in", Arrays.asList(Constants.hostList))).append("count",
                BasicDBObjectBuilder.start("$gte", countLimit).get()).append("timeStamp", /*curTs / 1000*/1441200600L);

        DBCursor cursor = collHrWiseArticleCount.find(qObject, fields);

        List<DBObject> catDataList = new ArrayList<>();

        int count = 0;
        while (cursor.hasNext()) {
            catDataList.add(cursor.next());
            count++;
        }
        System.out.println("Records read - " + count);


        JavaRDD<DBObject> catDataRDD = javaSparkContext.parallelize(catDataList);

        /***
         * Flatten the results fetched from DB.
         * Keys created here -
         *  1. host_cat
         *  2. host_cat (with cat value = All)
         */
        JavaPairRDD<String, Map<String, Object>> catPairRDD = catDataRDD.flatMapToPair(new PairFlatMapFunction<DBObject, String, Map<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            @SuppressWarnings("unchecked")
            public Iterable<Tuple2<String, Map<String, Object>>> call(DBObject dbObject) throws Exception {
                ArrayList<Tuple2<String, Map<String, Object>>> dataList = new ArrayList<Tuple2<String, Map<String, Object>>>();
                StringBuilder keyCatSpecific = new StringBuilder("host_").append(dbObject.get("host")).append("_cat_").append(dbObject.get("cat"));
                StringBuilder keyCatAll = new StringBuilder("host_").append(dbObject.get("host")).append("_cat_").append("All");
//                System.out.println("Key =" + key.toString() + "\nData =" + dbObject);
                dbObject.put("timeStamp", curTs);
                DBObject dbObjectCalAll = new BasicDBObject(dbObject.toMap());
                dbObjectCalAll.put("cat", "All");
                dataList.add(new Tuple2<String, Map<String, Object>>(keyCatSpecific.toString(), dbObject.toMap()));
                dataList.add(new Tuple2<String, Map<String, Object>>(keyCatAll.toString(), dbObjectCalAll.toMap()));
                return dataList;
            }
        });

        /***
         * Reduce the javaPairRDD on key created above
         */
        catPairRDD = catPairRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
                Long count1 = (Long) map1.get("count");
                Long count2 = (Long) map2.get("count");
                map1.put("count", count1 + count2);
                return map1;
            }
        });


        /***
         * Create new JavaPair rdd as - (null,object) to ensure system generated _id is used in mongo.
         */
        catPairRDD = catPairRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, String, Map<String, Object>>() {

            private static final long serialVersionUID = 4980477777146438979L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                return new Tuple2<String, Map<String, Object>>(null, stringMapTuple2._2());
            }
        });
/*
        catPairRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {

            private static final long serialVersionUID = 2527486900859618417L;

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
        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/" + dbName + "." + Config.CLICK_CAT_COLLECTION);
        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        catPairRDD.saveAsNewAPIHadoopFile("file:///notapplicable",
                Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);
        javaSparkContext.close();
    }
}
