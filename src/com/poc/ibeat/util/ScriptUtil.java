package com.poc.ibeat.util;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vibhav.Rohilla on 8/19/2015.
 */
public class ScriptUtil {
    public static boolean isEmpty(String object) {
        if (object == null) {
            return true;
        }
        if (object.length() <= 0) {
            return true;
        }
        return false;
    }

    public static long getHourSliceStamp() {
        Calendar c = new GregorianCalendar();
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTimeInMillis();
    }

    public static Map<String, Object> convertToMap(String msg,Logger logger) {
        ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> map = new HashMap<>();

        try {
            map = mapper.readValue(msg, new TypeReference<HashMap<String, Object>>() {
            });
            //logger.info(map);

        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }
        return map;
    }
    public static String getFormattedKeyStringDashboard(String host, String geoLocation, String isIndia, String referrer, String group, String articleId) {
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
