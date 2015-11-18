package com.poc.ibeat.script;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Created by Vibhav.Rohilla on 8/18/2015.
 */
public class Util {
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
}
