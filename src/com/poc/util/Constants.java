package com.poc.util;

import java.util.HashMap;
import java.util.Map;

public class Constants {
	public static final String COOKIE_NAME = "_iibeat_session";
	public static final String[] DOWNLOAD_TIME_SLOT = {"5", "10", "15","20", ">20"};
	
	public static final Map<String,String> hostMap = new HashMap<String,String>(){
		private static final long serialVersionUID = 1L;

		{
			put("timesofindia.indiatimes.com","toi");
			put("shop.indiatimes.com","shop");
			put("blogs.timesofindia.indiatimes.com","blog");
			put("luxpresso.com","luxpresso");
			put("photogallery.indiatimes.com","photogallery");
			put("idiva.com","idiva");
			put("maharashtratimes.indiatimes.com","maharashtra");
			put("zigwheels.com","zigwheels");
			put("economictimes.indiatimes.com","economictimes");
			put("guylife.com","guylife");
			put("navbharattimes.indiatimes.com","navbharattimes");
			put("filmfare.com","filmfare");
			put("hotklix.com","hotklix");
			put("femina.in","femina");
			put("vijaykarnataka.indiatimes.com","vijaykarnataka");
			put("zoomtv.indiatimes.com","zoomtv");
			put("eisamay.indiatimes.com","eisamay");
			put("timescrest.com","timescrest");
			put("epaper.timesofindia.com","epaper");
			put("mumbaimirror.com","mumbaimirror");
			put("blogs.eisamay.indiatimes.com","blogeisamay");
			put("indiatimes.com","indiatimes");
			put("itimes.com","itimes");
			put("beautypageants.indiatimes.com","beautypageants");
			put("lifehacker.co.in","lifehacker");
			put("rocketoff.com","rocketoff");
			put("happytrips.com","happytrips");
			put("gaana.com","gaana");
			put("bombaytimes.com","bombaytimes");
			put("navgujaratsamay.indiatimes.com","navgujaratsamay");
			put("healthmeup.com","healthmeup");
			put("gizmodo.in","gizmodo");
			put("businessinsider.in","businessinsider");
			put("adageindia.in","adageindia");
			put("in.techradar.com","techradar");
			put("blogs.malayalam.samayam.com","blogmalyalam");
			put("blogs.tamil.samayam.com","blogtamil");
			put("blogs.telugu.samayam.com","blogtelugu");
		}
	};
	
	
	public static final String[]  hostList = {"timesofindia.indiatimes.com",
												"shop.indiatimes.com",
												"blogs.timesofindia.indiatimes.com",
												"luxpresso.com",
												"photogallery.indiatimes.com",
												"idiva.com",
												"maharashtratimes.indiatimes.com",
												"zigwheels.com",
												"economictimes.indiatimes.com",
												"guylife.com",
												"navbharattimes.indiatimes.com",
												"filmfare.com",
												"hotklix.com",
												"femina.in",
												"vijaykarnataka.indiatimes.com",
												"zoomtv.indiatimes.com",
												"eisamay.indiatimes.com",
												"timescrest.com",
												"epaper.timesofindia.com",
												"mumbaimirror.com",
												"blogs.eisamay.indiatimes.com",
												"indiatimes.com",
												"itimes.com",
												"beautypageants.indiatimes.com",
												"lifehacker.co.in",
												"happytrips.com",
												"rocketoff.com",
												"gaana.com",
												"photogallery.navbharattimes.indiatimes.com",
												"photogallery.maharashtratimes.indiatimes.com",
												"photogallery.eisamay.indiatimes.com",
												"photogallery.vijaykarnataka.indiatimes.com",
												"photogallery.navgujaratsamay.indiatimes.com",
												"navgujaratsamay.indiatimes.com",
												"bombaytimes.com",
												"healthmeup.com",
												"gizmodo.in",
												"businessinsider.in",
												"adageindia.in",
												"in.techradar.com",
												"blogs.malayalam.samayam.com",
												"blogs.tamil.samayam.com",
												"blogs.telugu.samayam.com"
												};
	
	public static final String[]  businessHostList = {
			"timesofindia.indiatimes.com",
			"economictimes.indiatimes.com",
			"navbharattimes.indiatimes.com",
			"photogallery.indiatimes.com"/*,
			"blogs.timesofindia.indiatimes.com",
			"maharashtratimes.indiatimes.com",
			"hotklix.com",
			"vijaykarnataka.indiatimes.com",
			"epaper.timesofindia.com"*/
		};
	
	public static final String[] bigHostList = {
		"timesofindia.indiatimes.com",
		"photogallery.indiatimes.com",
		"maharashtratimes.indiatimes.com",
		"economictimes.indiatimes.com",
		"navbharattimes.indiatimes.com",
		//"beautypageants.indiatimes.com",
		//"lifehacker.co.in",
		"zigwheels.com"
		/*"happytrips.com",
		"rocketoff.com",
		"gaana.com",
		"photogallery.navbharattimes.indiatimes.com",
		"photogallery.maharashtratimes.indiatimes.com",
		"photogallery.eisamay.indiatimes.com",
		"photogallery.vijaykarnataka.indiatimes.com",
		"photogallery.navgujaratsamay.indiatimes.com",
		"navgujaratsamay.indiatimes.com",
		"bombaytimes.com",
		"healthmeup.com",
		"gizmodo.in",
		"businessinsider.in",
		"adageindia.in",
		"in.techradar.com"*/
	};
	
	public static final String[]  utmHostList = {
		"lifehacker.co.in",
		"happytrips.com"
	};
	
	public static final String[]  socialHostList = {
		"indiatimes.com","timesofindia.indiatimes.com"
	};
	
	public static final String[]  tagHostList = {
		"photogallery.indiatimes.com"
	};
	
	// OEM Merchants
	public static final String[] oemMerchantList = { "TOI", "ET", "NBT" };
	
	public static final int BIGHOST_WEEK_LIMIT = 30;
	public static final int OTHERHOST_WEEK_LIMIT = 5;
	
	public static final int BIGHOST_MONTH_LIMIT = 70;
	public static final int OTHERHOST_MONTH_LIMIT = 5;
	
	public static final int BIGHOST_YEAR_LIMIT = 300;
	public static final int OTHERHOST_YEAR_LIMIT = 5;
	
	public static final int BIGHOST_DAY_LIMIT = 20;
	public static final int OTHERHOST_DAY_LIMIT = 1;
	
	public static final int BIGHOST_HOUR_LIMIT = 2;
	public static final int OTHERHOST_HOUR_LIMIT = 1;
	
	public static final int TAG_AGGHOUR_COUNT_LIMIT = 5;
	public static final int TAG_AGGDAYMONTH_COUNT_LIMIT = 10;
}


