package com.poc.mongo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.poc.ibeat.script.Config;
import com.poc.ibeat.util.DateTimeUtils;

/**
 * Created by Vibhav.Rohilla on 9/3/2015.
 */
public class MongoRead implements Serializable{

	private static final long serialVersionUID = 7343014322221994261L;

	private MongoRead() {
    }

    public static ArrayList<DBObject> getIntervalDataFromMongo(String collection, int interval,String dbName) {
        MongoClient mongo = MongoClientSingleton.getInstance();
        DB db = mongo.getDB(dbName);

        DBCollection coll = db.getCollection(collection);
        DBObject queryObject = new BasicDBObject().append("timeStamp", BasicDBObjectBuilder.start("$gte", getStartTimeStampSlice(interval)).add("$lt", getCurrentTimeStampSlice(interval)).get());
        DBCursor cursor = coll.find(queryObject);
        ArrayList<DBObject> dataList = new ArrayList<>();
        while (cursor.hasNext()) {
            dataList.add(cursor.next());
        }
        return  dataList;

    }

    public static Map<String, BasicDBObject> getAllData(String collection,String dbName,String key,String value) {
        MongoClient mongo = MongoClientSingleton.getInstance();
        DB db = mongo.getDB(dbName);

        DBCollection coll = db.getCollection(collection);
        Map<String, BasicDBObject> dataMap= new HashMap<>();

        DBCursor cursor = coll.find();
        DBObject object;
        while (cursor.hasNext()) {
            object = cursor.next();
            dataMap.put(object.get(key).toString(), (BasicDBObject) object.get(value));

        }
        return  dataMap;

    }

    private static long getStartTimeStampSlice(int interval) {
        switch (interval) {
            case Config.MINUTE:
                long startTs = DateTimeUtils.get5MinuteSliceStamp(5);
                return (startTs - Config.MINUTE_IN_MILLISEC);
            case Config.HOUR:
                startTs = DateTimeUtils.getHourSliceStamp();
                return (startTs - Config.HOUR_IN_MILLISEC / 2);
            case Config.WEEK:
                startTs = DateTimeUtils.getWeeklySliceStamp();
                return (startTs - Config.WEEK_IN_MILLISEC / 2);
            case Config.MONTH:
                startTs = DateTimeUtils.getMonthlySliceStamp();
                return (startTs - Config.MONTH_IN_MILLISEC / 2);
            default:
                System.exit(0);
                return 0;
        }
    }

    public static long getCurrentTimeStampSlice(int interval) {
        switch (interval) {
            case Config.MINUTE:
                return DateTimeUtils.get5MinuteSliceStamp(5);
            case Config.HOUR:
                return DateTimeUtils.getHourSliceStamp();
            case Config.WEEK:
                return DateTimeUtils.getWeeklySliceStamp();
            case Config.MONTH:
                return DateTimeUtils.getMonthlySliceStamp();
            default:
                System.exit(0);
                return 0;
        }
    }
}
