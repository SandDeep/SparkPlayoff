package com.test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoOutputFormat;
import com.poc.ibeat.script.Config;
import com.poc.ibeat.util.DateTimeUtils;
import com.poc.util.SparkProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.bson.BSONObject;
import scala.Tuple2;

import java.util.Calendar;
import java.util.Map;

/**
 * Created by Vibhav.Rohilla on 9/3/2015.
 */
public class HadoopMongoRead {
    private static void groupAndSaveForHour() {
        Long startTs = DateTimeUtils.getHourSliceStamp();
        ;

        JavaSparkContext sc = new JavaSparkContext("local", "Hour Script");
        Configuration config = new Configuration();
//        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/ibeat20150428." + Config.HOURLY_TAGS_COLLECTION);
//        config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/ibeat20150428." + Config.HOURLY_TAGS_COLLECTION);
        config.set("mongo.input.uri", "mongodb://192.168.24.204:27017/iBeat." + "timedCount");
        config.set("mongo.output.uri", "mongodb://192.168.24.204:27017/iBeat." + "timedCount");
        config.set("mongo.input.query", getQueryStringForHourScript());
//        config.set("mongo.input.fields", getFieldsStringFroHourScript());
        config.set("mongo.input.limit", "100000");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);

        mongoRDD.foreach(new VoidFunction<Tuple2<Object, BSONObject>>() {
            private static final long serialVersionUID = -5447455949365193895L;

            @Override
            public void call(Tuple2<Object, BSONObject> tuple_2) throws Exception {
//                System.out.println("tuple_2 - " + tuple_2._2());
            }
        });

//        JavaPairRDD<String, Map<String, Object>> keyDataRDD = mongoRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>, String, Map<String, Object>>() {
//            @Override
//            public Tuple2<String, Map<String, Object>> call(Tuple2<Object, BSONObject> tuple) throws Exception {
//
//                StringBuilder key = new StringBuilder("host_").append(tuple._2().get("host")).append("_tag_").append(tuple._2().get("tags"));
//                System.out.println("Key =" + key.toString() + "\nData =" + tuple._2());
//                return new Tuple2<String, Map<String, Object>>(key.toString(), tuple._2().toMap());
//            }
//        });
//
//
//        keyDataRDD = keyDataRDD.reduceByKey(new Function2<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
//            @Override
//            public Map<String, Object> call(Map<String, Object> map1, Map<String, Object> map2) throws Exception {
//                Long count1 = (Long) map1.get("count");
//                Long count2 = (Long) map2.get("count");
//                BasicDBList mergedArticles = mergeDBListsForHour((BasicDBList) map1.get("articleId"), (BasicDBList) map2.get("articleId"));
//
//                map1.put("count", count1 + count2);
//                map1.put("articleId", mergedArticles);
//                return map1;
//            }
//        });
//
//        keyDataRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {
//            @Override
//            public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
//                logger.info("red Data" + stringMapTuple2._2());
//            }
//        });
//
//        JavaPairRDD<Object, BSONObject> dbData = keyDataRDD.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, Object, BSONObject>() {
//            @Override
//            public Tuple2<Object, BSONObject> call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
//                DBObject dbObject = null;
//                String host = (String) stringMapTuple2._2().get("host");
//                String[] arr = stringMapTuple2._1.split("_");
//
//                String tags = arr[3];
//                if (tags == null) {
//                    tags = "";
//                }
//                long count = (long) stringMapTuple2._2().get("count");
//                long catIdHash = (long) (stringMapTuple2._2().get("catIdHash") == null ? 0L : stringMapTuple2._2().get("catIdHash"));
//                String cat = (String) stringMapTuple2._2().get("cat");
//                /*BasicDBList articleIdDBList = new BasicDBList();
//                Map<String, Long> articleIdMap = (Map<String, Long>) stringMapTuple2._2().get("articleId");
//                if (articleIdMap != null || articleIdMap.isEmpty()) {
//                    BasicDBObject articleIdAndCount;
//                    for (String articleId : articleIdMap.keySet()) {
//                        articleIdAndCount = new BasicDBObject();
//                        articleIdAndCount.put("articleId", articleId);
//                        articleIdAndCount.put("count", articleIdMap.get(articleId));
//                        articleIdDBList.add(articleIdAndCount);
//                    }
//                }*/
//                Integer cTagValue = null;
//                if (tags != null && !tags.isEmpty()) {
//                    cTagValue = tags.hashCode();
//                }
//
//                dbObject = new BasicDBObject("articleId", stringMapTuple2._2().get("articleId"))
//                        .append("host", host)
//                        .append("count", count)
//                        .append("cat", cat)
//                        .append("catIds", stringMapTuple2._2().get("catIds"))
//                        .append("timeStamp", startTs / 1000)
//                        .append("tags", tags)
//                        .append("cTag", cTagValue)
//                        .append("catIdHash", catIdHash);
//
//                return new Tuple2<Object, BSONObject>(null, dbObject);
//            }
//        });
//
//
//        Configuration minuteScriptMongoConf = new Configuration();
//        minuteScriptMongoConf.set("mongo.output.uri", "mongodb://" + SparkProperties.MONGO_IP + ":" + SparkProperties.MONGO_PORT + "/test." + Config.HOURLY_TAG_COUNT);
//        minuteScriptMongoConf.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
//
//        /***
//         * Push every RDD to the DB
//         */
//        dbData.saveAsNewAPIHadoopFile("file:///notapplicable", Object.class, BSONObject.class, MongoOutputFormat.class, minuteScriptMongoConf);

//        DBCollection hourCollection = db.getCollection(Config.HOURLY_TAGS_COLLECTION);
    }

    /**
     * Method returns the query object with the filter params needed to fetch 1 hour Data.
     *
     * @return String - QueryObject
     */
    private static String getQueryStringForHourScript() {
        long currentTime = Calendar.getInstance().getTimeInMillis(); //test Data
        long startTime = 0L;//test Data

        DBObject qObject = new BasicDBObject().append("host","timesofindia.indiatimes.com").append("count",2);
        return qObject.toString();
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

    public static void main(String[] args) {
        groupAndSaveForHour();
    }

}
