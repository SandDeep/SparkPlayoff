package com.poc.ibeat.script;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.poc.ibeat.util.DateTimeUtils;

/**
 * Created by Vibhav.Rohilla on 8/18/2015.
 */
public class DashboardDataMongoHandler {
    private static final MongoClient mongoHistory = new MongoClient(Config.HOST_HISTORICAL,
            MongoClientOptions.builder().connectionsPerHost(10)
                    .threadsAllowedToBlockForConnectionMultiplier(15)
                    .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                    .build());
    private static DB dashboardDB = mongoHistory.getDB(Config.DBNAME_DASHBOARD_DB);

    public static void groupByReferrer(Map dataMap) {

        Long time = System.currentTimeMillis();
        long timeStamp = Util.getHourSliceStamp() + 3600000;

        if (dataMap != null) {

           /* DBObject referrerDBObject = getObjectFromDB(dataMap, Config.REFERER_COLLECTION);
            if (referrerDBObject == null) {
                referrerDBObject = createNewReferrerDBObject(dataMap);
            }*/

            int sum = 0;
            Object newDataCount = dataMap.get("count");
            try {
                sum = ((Double) Double.parseDouble(newDataCount.toString()))
                        .intValue();
            } catch (Exception e) {
                sum = ((Double) newDataCount).intValue();
            }

//            String cat = (String) referrerDBObject.get("cat");
            BasicDBObject qdb = getReferrerQueryObject(timeStamp,dataMap);

            BasicDBObject updateObj = new BasicDBObject("$setOnInsert",
                    new BasicDBObject/*("cat", cat).append*/("tsd", new Date(timeStamp * 2))).append("$inc",
                    new BasicDBObject("count", sum));
            DB dashboardDB = mongoHistory.getDB(Config.DBNAME_DASHBOARD_DB);
            dashboardDB.getCollection(Config.REFERER_COLLECTION).update(qdb,
                    updateObj, true, false);
        }

        System.out.println(" Update time for Referrer data" + (System.currentTimeMillis() - time));
    }

    private static BasicDBObject getReferrerQueryObject(long timeStamp, Map dataMap) {
        String referer = (String) dataMap.get("referer");
        if (referer == null || referer.equals("")) {
            referer = "none";
        }
        return new BasicDBObject("timeStamp", timeStamp)
                .append("articleId", dataMap.get("articleId")).append("host", dataMap.get("host"))
                .append("referer",referer);
    }

    @Deprecated
    private static DBObject createNewReferrerDBObject(Map map) {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("host", map.get("host"));
        dbObject.put("geoLocation", map.get("geoLocation"));
        dbObject.put("isIndia", map.get("isIndia"));
        dbObject.put("referer", map.get("referer"));
        dbObject.put("group", map.get("group"));
        dbObject.put("articleId", map.get("articleId"));
        dbObject.put("count", map.get("count"));
        dbObject.put("cat", "Home");
        return dbObject;
    }


    public static void groupByResponse(Map dataMap) {

        Long time = System.currentTimeMillis();
        long timeStamp = DateTimeUtils.getHourSliceStamp() + 3600000;
        Set<BasicDBObject> newResQ = new HashSet<BasicDBObject>();
        if (dataMap != null) {

/*            DBObject respDBObject = getObjectFromDB(dataMap, Config.RESPONSE_COLLECTION);
            if (respDBObject == null) {
                respDBObject = createNewResponseDBObject(dataMap);
            }

            Object host = dataMap.get("host");
            int groupVal = 4;
            try {
                groupVal = ((Double) Double.parseDouble(respDBObject.get("group")
                        .toString())).intValue();
            } catch (Exception e) {
                groupVal = ((Double) respDBObject.get("group")).intValue();
            }
            Object articleId = respDBObject.get("articleId");
*/
            int sum = 0;
            Object obj = dataMap.get("count");
//            String cat = (String) respDBObject.get("cat");
            try {
                sum = ((Double) Double.parseDouble(obj.toString()))
                        .intValue();
            } catch (Exception e) {
                sum = ((Double) obj).intValue();
            }
/*
            long totTm = 0l;
            Object pgDnTime = respDBObject.get("pgDownloadTime");
            if (((BasicDBList) (pgDnTime) != null)) {
                for (Object bdb : (BasicDBList) (pgDnTime)) {
                    totTm = totTm + ((Double) Double.parseDouble(bdb.toString())).longValue();
                }
            }
*/

            BasicDBObject qdb = getResponseQueryObject(timeStamp,dataMap);
            newResQ.add(qdb);

            BasicDBObject updateObj = new BasicDBObject("$setOnInsert",
                    new BasicDBObject/*("cat", cat).append*/("tsd", new Date(timeStamp * 2))).append("$inc",
                    new BasicDBObject("count", sum)/*.append("totTm",(long) (totTm / 1000))*/);
            DB dashboardDB = mongoHistory.getDB(Config.DBNAME_DASHBOARD_DB);
            dashboardDB.getCollection(Config.RESPONSE_COLLECTION).update(qdb,
                    updateObj, true, false);
        }


        System.out.println(" Update time for Response data" + (System.currentTimeMillis() - time));
    }

    private static BasicDBObject getResponseQueryObject(long timeStamp, Map dataMap) {
        return new BasicDBObject("timeStamp", timeStamp)
                .append("articleId", dataMap.get("articleId")).append("host", dataMap.get("host"))
                .append("group", dataMap.get("group"));

    }

    @Deprecated
    private static DBObject createNewResponseDBObject(Map map) {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("host", map.get("host"));
        dbObject.put("geoLocation", map.get("geoLocation"));
        dbObject.put("isIndia", map.get("isIndia"));
        dbObject.put("referer", map.get("referer"));
        dbObject.put("group", map.get("group"));
        dbObject.put("articleId", map.get("articleId"));
        dbObject.put("count", map.get("count"));
        dbObject.put("cat", "Home");
        return dbObject;
    }

    public static void groupByLocation(Map dataMap) {

        Long time = System.currentTimeMillis();
        long timeStamp = DateTimeUtils.getHourSliceStamp() + 3600000;
        if (dataMap != null) {
/*

            DBObject locationDBObject = getObjectFromDB(dataMap, Config.LOCATION_COLLECTION);
            if (locationDBObject == null) {
                locationDBObject = createNewLocationDBObject(dataMap);
            }
*/

            int sum = 0;
            Object obj = dataMap.get("count");
//            String cat = (String) locationDBObject.get("cat");
            try {
                sum = ((Double) Double.parseDouble(obj
                        .toString())).intValue();
            } catch (Exception e) {
                sum = ((Double) obj).intValue();
            }
            BasicDBObject qdb = getLocationDBObject(timeStamp,dataMap);

            BasicDBObject updateObj = new BasicDBObject("$setOnInsert", new BasicDBObject/*("cat", cat).append*/("tsd", new Date(timeStamp * 2)))
                    .append("$inc", new BasicDBObject("count", sum));
            DB dashboardDB = mongoHistory.getDB(Config.DBNAME_DASHBOARD_DB);
            dashboardDB.getCollection(Config.LOCATION_COLLECTION).update(qdb, updateObj, true, false);
        }
        System.out.println(" Update time for Location data" + (System.currentTimeMillis() - time));
    }

    private static BasicDBObject getLocationDBObject(long timeStamp, Map dataMap) {
        return new BasicDBObject("timeStamp", timeStamp).append("articleId", dataMap.get("articleId"))
                .append("host", dataMap.get("host"))
                .append("isIndia", dataMap.get("isIndia"))
                .append("location", dataMap.get("geoLocation"));
    }

    @Deprecated
    private static DBObject createNewLocationDBObject(Map map) {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("host", map.get("host"));
        dbObject.put("geoLocation", map.get("geoLocation"));
        dbObject.put("isIndia", map.get("isIndia"));
        dbObject.put("referer", map.get("referer"));
        dbObject.put("group", map.get("group"));
        dbObject.put("articleId", map.get("articleId"));
        dbObject.put("count", map.get("count"));
        dbObject.put("cat", "Home");
        return dbObject;
    }


    @Deprecated
    private static DBObject getObjectFromDB(Map map, String collection) {
        BasicDBObject searchObject = new BasicDBObject();
        searchObject.put("host", map.get("host"));
        searchObject.put("geoLocation", map.get("geoLocation"));
        searchObject.put("isIndia", map.get("isIndia"));
        searchObject.put("referer", map.get("referer"));
        searchObject.put("group", map.get("group"));
        searchObject.put("articleId", map.get("articleId"));
        DBObject searchResult = dashboardDB.getCollection(collection).findOne(searchObject);
        System.out.println("=============>DB Search Result - " + searchResult);
        return searchResult;
    }
}
