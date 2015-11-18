package com.poc.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.ServerAddress;

public final class Config {

	public static final String HOST_SERVER_ADDRESS = "192.168.33.82";
	public static final Integer HOST_SERVER_PORT = 27011;

	public static final String DB_SERVER_ADDRESS = "192.168.24.204";
	public static final String LOCAL_DB_SERVER_ADDRESS = "127.0.0.1";
	public static final String DBNAME = "iBeat";
	public static final String HOURLY_SOURCE_COLLECTION = "timedCount";
	public static final String OFFSET_COLLECTION = "kafkaOffset";

	public static List<ServerAddress> HOST_HISTORICAL = new ArrayList<ServerAddress>() {
		private static final long serialVersionUID = 1L;
		{
			try {
				add(new ServerAddress("192.168.33.209", 27017));
				add(new ServerAddress("192.168.33.210", 27017));
				add(new ServerAddress("192.168.33.211", 27017));
				add(new ServerAddress("127.0.0.1", 27017));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
	};
	
	public static final String[] HOST_LIST = { "photogallery.indiatimes.com",
			"maharashtratimes.indiatimes.com", "timesofindia.indiatimes.com",
			"zigwheels.com", "shop.indiatimes.com",
			"navbharattimes.indiatimes.com", "indiatimes.com", "luxpresso.com",
			"zoomtv.indiatimes.com", "lifehacker.co.in",
			"beautypageants.indiatimes.com",
			"blogs.timesofindia.indiatimes.com", "filmfare.com",
			"eisamay.indiatimes.com", "mumbaimirror.com", "itimes.com",
			"epaper.timesofindia.com", "healthmeup.com", "happytrips.com",
			"vijaykarnataka.indiatimes.com", "timescrest.com", "guylife.com",
			"femina.in" };

	public static Integer threadPoolSize = 10;

	public static boolean chkNull(String text) {
		// System.out.println("Value : " + text);
		if (text != null && !text.trim().equals("")
				&& !text.equals("undefined")) {
			return false;
		}
		return true;
	}

	public static final long FIVE_MINUTE_IN_MILLISEC = 300000l;
	public static final long HOUR_IN_MILLISEC = 300000 * 12l;
	public static final long DAY_IN_MILLISEC = 300000 * 12 * 24l;

	// Constants
	public static final int MINUTE = 1;
	public static final int HOUR = 2;
	public static final int DAY = 3;
	public static final int WEEK = 4;
	public static final int MONTH = 5;
	public static final int CUSTOM = 6;
	public static final int MONTH_WEEK = 7;
	public static final int FOREVER = 8;
	public static final int YEAR = 9;
}
