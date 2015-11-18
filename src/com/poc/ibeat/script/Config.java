package com.poc.ibeat.script;

import com.mongodb.ServerAddress;
import com.poc.ibeat.util.DateTimeUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public final class Config {
	
	 public static final String IBEAT_HOST_SERVER = "ibeat.indiatimes.com";
	//For Analytics logging servers
	/* public static List<ServerAddress> writeReadHostAddrs = new ArrayList<ServerAddress>(){
		private static final long serialVersionUID = 1L;
		{
			try {
				add(new ServerAddress( "192.168.22.106" ,27017));
				add(new ServerAddress( "192.168.33.82" ,27017));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
	 };*/
	
	//For Analytics UI and  IBEAT API server
	//Read api Mongos servers
	public static List<ServerAddress> writeReadHostAddrs = new ArrayList<ServerAddress>() {
		private static final long serialVersionUID = 1L;
		{
			try {
				add(new ServerAddress("192.168.22.61", 27017));
				add(new ServerAddress("192.168.33.144", 27017));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
	};
	
	
	/****************************************************************
	 *  					CONSTANTS								*
	 ****************************************************************/
	 
	
	public static String HOST_SCRIPT   = "localhost";
	
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
	
	public static String HOST_ARTICLE_READ   = "192.168.22.55";
	public static String HOST_ARTICLE_WRITE   = "192.168.22.55";
	public static String HOST_ARTICLE_SCRIPT   = "localhost";
	
	public static String DBNAME = "ibeat";
	public static String DBNAME_BUSINESS = "ibeatBusiness";
	public static String DBNAME_ARTICLE_DB = "ibeatArticleDB";
	public static String DBNAME_AUTHOR_DB = "ibeatAuthor";
	public static String DBNAME_WEEK_DB = "ibeatWeek";
	public static String DBNAME_MONTH_DB = "ibeatMonth";
	public static String DBNAME_HISTORY_DB = "ibeatHistory";
	public static String DBNAME_PROCESS_DB = "ibeatProcessDB";
	public static String DBNAME_ANALYTIC_DB = "analytics";
	
	public static String DBNAME_CACHE_DB = "ibeatCacheDB";
	public static String DBNAME_DASHBOARD_DB = "ibeatDashboard";
	
	// Mongo DB Collections Names 
	public static final String MIN_SOURCE_COLLECTION = "pageTrendLog";
	public static final String APP_LOG_COLLECTION = "appTrendLog";
	public static final String ARTICLE_MASTER_COLLECTION = "articleMaster";
	public static final String HOURLY_SOURCE_COLLECTION = "timedCount";
	public static final String HOURLY_TAGS_COLLECTION = "tagsAggrtCount";
	public static final String HOURLY_LOCATION_SOURCE_COLLECTION = "timedLocationCount";
	public static final String DAILY_SOURCE_COLLECTION = "dayCount";
	
	public static final String WEEKLY_LOCATION_SOURCE_COLLECTION = "weekLocationCount";
	public static final String MONTHLY_LOCATION_SOURCE_COLLECTION = "monthLocationCount";
	public static final String STATS_COLLECTION = "statsData";
	public static final String STATS_LOCATION_COLLECTION = "statsLocationData";
	public static final String CLICK_LOCATION_COUNT_COLLECTION = "clickLocationCount";
	public static final String LOCATION_COLLECTION = "locationCount";
	public static final String REFERER_COLLECTION = "refererCount";
	public static final String RESPONSE_COLLECTION = "dgrpCount";
	public static final String CAT_STATS_COLLECTION = "catJob";
	public static final String CAT_LOCATION_STATS_COLLECTION = "catLocationJob";
	public static final String CLICK_CAT_COLLECTION = "clicksByCat";
	public static final String CLICK_CAT_LOCATION_COLLECTION = "clicksByCatLocation";
	public static final String RESP_CAT_COLLECTION = "responseByCat";
	public static final String RESP_CAT_LOCATION_COLLECTION = "responseByCatLocation";
	public static final String LOC_CAT_COLLECTION = "locationByCat";
	public static final String REF_CAT_COLLECTION = "refererByCat";
	public static final String REF_CAT_LOCATION_COLLECTION = "refererByCatLocation";
	public static final String ARTICLE_COUNT_COLLECTION =  "ArticleHistory";
	public static final String ARTICLE_LOCATION_COUNT_COLLECTION =  "ArticleLocationCount";
	//public static final String CURRENT_WEEK_ARTICLE_COUNT_COLLECTION =  "currentWeekArticleTrend";
	public static final String WEEK_ARTICLE_COUNT_COLLECTION =  "WeekArticleTrend";
	public static final String MONTH_ARTICLE_COUNT_COLLECTION =  "MonthArticleTrend";
	public static final String CURRENT_WEEK_TAGS_COUNT_COLLECTION =  "currentWeekTagsTrend";
	public static final String CURRENT_WEEK_ARTICLE_LOCATION_COUNT_COLLECTION =  "currentWeekArticleLocationCount";
//	public static final String CURRENT_MONTH_ARTICLE_COUNT_COLLECTION =  "currentMonthArticleTrend";
	public static final String CURRENT_MONTH_TAGS_COUNT_COLLECTION =  "currentMonthTagsTrend";
	public static final String CURRENT_MONTH_ARTICLE_LOCATION_COUNT_COLLECTION =  "currentMonthArticleLocationCount";
	public static final String CURRENT_WEEK_CLICK_CAT_COUNT_COLLECTION =  "currentWeekClicksByCatCount";
	public static final String CURRENT_WEEK_CLICK_CAT_LOCATION_COUNT_COLLECTION =  "currentWeekClicksByCatLocationCount";
	public static final String CURRENT_MONTH_CLICK_CAT_COUNT_COLLECTION =  "currentMonthClicksByCatCount";
	public static final String CURRENT_MONTH_CLICK_CAT_LOCATION_COUNT_COLLECTION =  "currentMonthClicksLocationByCatCount";
	public static final String DAILY_CLICK_CAT_COUNT_COLLECTION =  "dayClicksByCatCount";
	public static final String DAILY_CLICK_CAT_LOCATION_COUNT_COLLECTION =  "dayClicksByCatLocationCount";
	public static final String DAILY_REFERER_CAT_COUNT_COLLECTION =  "dayRefererByCatCount";
	public static final String DAILY_REFERER_CAT_LOCATION_COUNT_COLLECTION =  "dayRefererByCatLocationCount";
	public static final String CURRENT_WEEK_REFERER_CAT_COUNT_COLLECTION =  "currentWeekRefererByCatCount";
	public static final String CURRENT_WEEK_REFERER_CAT_LOCATION_COUNT_COLLECTION =  "currentWeekRefererByCatLocationCount";
	public static final String CURRENT_MONTH_REFERER_CAT_COUNT_COLLECTION =  "currentMonthRefererByCatCount";
	public static final String CURRENT_MONTH_REFERER_CAT_LOCATION_COUNT_COLLECTION =  "currentMonthRefererByCatLocationCount";
	public static final String DAILY_RESPONSE_CAT_COUNT_COLLECTION =  "dayResponseByCatCount";
	public static final String DAILY_RESPONSE_CAT_LOCATION_COUNT_COLLECTION =  "dayResponseByCatLocationCount";
	public static final String CURRENT_WEEK_RESPONSE_CAT_COUNT_COLLECTION =  "currentWeekResponseByCatCount";
	public static final String CURRENT_WEEK_RESPONSE_CAT_LOCATION_COUNT_COLLECTION =  "currentWeekResponseByCatLocationCount";
	public static final String CURRENT_MONTH_RESPONSE_CAT_COUNT_COLLECTION =  "currentMonthResponseByCatCount";
	public static final String CURRENT_MONTH_RESPONSE_CAT_LOCATION_COUNT_COLLECTION =  "currentMonthResponseByCatLocationCount";
	public static final String CURRENT_WEEK_LOC_CAT_COUNT_COLLECTION =  "currentWeekLocationByCatCount";
	public static final String CURRENT_MONTH_LOC_CAT_COUNT_COLLECTION =  "currentMonthLocationByCatCount";
	public static final String DAILY_LOC_CAT_COLLECTION =  "dayLocationByCatCount";
	public static final String ACTION_COUNT_COLLECTION =  "actionCount";
	public static final String TOTAL_ACTION_COUNT_COLLECTION =  "totalActionCount";
	public static final String DAILY_ACTION_COUNT_COLLECTION =  "dayActionCount";
	public static final String CUSTOM_LOCATION_COUNT_COLLECTION =  "hoursAggregationLocationCount";
	public static final String CUSTOM_ARTICLE_COUNT_COLLECTION =  "hoursAggregationArticleCount";
	public static final String CUSTOM_ARTICLE_LOCATION_COUNT_COLLECTION =  "hoursAggregationArticleLocationCount";
	public static final String MIN_ARTICLE_COUNT_COLLECTION =  "minsAggregationArticleCount";
	public static final String MIN_ARTICLE_LOCATION_COUNT_COLLECTION =  "minsAggregationArticleLocationCount";
	public static final String CUSTOM_REFERER_COUNT_COLLECTION =  "hoursAggregationRefererCount";
	public static final String CUSTOM_RESPONSE_COUNT_COLLECTION =  "hoursAggregationResponseCount";
	public static final String CUSTOM_CLICKSCAT_COUNT_COLLECTION =  "hoursAggregationClicksCatCount";
	public static final String CUSTOM_ACTION_COUNT_COLLECTION =  "hoursAggregationActionCount";
	public static final String VISITOR_COLLECTION =  "visitor";
	public static final String ARTICLE_POSITION_COLLECTION =  "articlePosition";
	public static final String BUSINESS_CLIENT_SESSION_CAT =  "ClientSessionCat";
	public static final String RECENT_ARTICLE_COUNT_COLLECTION =  "RecentArticles";
	public static final String UTM_COLLECTION =  "UTMCollection";
	public static final String SP_COUNT_COLLECTION =  "SPCountCollection";
	public static final Long SP_COUNTER =  100l;
	public static final String KEYWORD_COLLECTION =  "keyWordCollection";
	public static final String USER_AGENT_COLLECTION =  "userAgentCollection";
	public static final String HOST_DEVICE_COLLECTION = "hostDeviceMapColl";
	public static final String SOCIAL_GRAPH_DATA_COLLECTION =  "SocialGraphData";
	public static final String USER_FREQUENCY_COLLECTION =  "userFrequencyCollection";
	public static final String HOST_DEVICE_AGG_COLLECTION = "hostDeviceMap";


	public static final String SESSION_ID_TAGS_COLLECTION =  "sessionIdTags";
	
	public static final String BUSINESS_DATA_UPLOAD_URL = "http://192.168.27.95/uploads/dataChunk";
	public static final String BUSINESS_DATA_TEST_UPLOAD_URL = "http://192.168.27.95/uploadsnew/";
	
	public static final String UTM_DATA_UPLOAD_URL = "http://192.168.27.95/jsonCSVUploads/";
	
	public static final String BUSINESS_ARTICLE_MAPPING_DATA_UPLOAD_URL = "http://192.168.27.95/articleIdUploads/articleMapps";
	
	public static final String BUSINESS_ERRO_LOG_URL = "http://192.168.27.95/cgi-bin/file_upload_log.pl";
	
	public static final String PRVIOUS_DAY_DB = DateTimeUtils.getDateYYYYMMDD(DateTimeUtils.get5MinuteSliceStamp(5)- com.poc.ibeat.script.Config.DAY_IN_MILLISEC);
	
	public static final String TODAY_DAY_DB = DateTimeUtils.getDateYYYYMMDD(DateTimeUtils.get5MinuteSliceStamp(5));
	
	public static final String GRAPH_CHACHE_COL =  "graphCache";
	
	public static final String OEM_DATA_UPLOAD_URL = "http://192.168.24.132/uploads/track/OEMDATA";
	//public static final String OEM_DATA = "OEMDATA";
	
	// Week Collection
	public static final String WEEKLY_SOURCE_COLLECTION = "weekCount";
	public static final String WEEKLY_SOURCE_TAGS_COLLECTION = "weekTagsCount";
	
	// Monthly Collection
	public static final String MONTHLY_SOURCE_COLLECTION = "monthCount";
	public static final String MONTHLY_SOURCE_TAGS__COLLECTION = "monthTagsCount";

	// History Collection
	public static final String DAY_ARTICLE_COUNT =  "DayArticleCount";
	public static final String SOCIAL_DATA_COUNT =  "SocialDataCount";
	public static final String HOURLY_ARTICLE_COUNT =  "HourwiseArticleCount";
	public static final String HOURLY_TAG_COUNT =  "HourwiseTagCount";
	public static final String APP_ACTIVATION_COLLECTION = "deviceActivation";
	public static final String CATID_HASH_COLLECTION = "catidHash";
	public static final String DAILY_ARTICLE_COUNT =  "DaywiseArticleCount";
	
	// Analytics Collection
	public static final String USER_MAPPING =  "UserMapping";
	
	// Cache Collections
	public static final String DAY_CACHE =  "DayCache";
	public static final String WEEK_CACHE =  "WeekCache";
		
	// Constants
	public static final int MINUTE	= 1;
	public static final int HOUR   	= 2;
	public static final int DAY   	= 3;
	public static final int WEEK   	= 4;
	public static final int MONTH   = 5;
	public static final int CUSTOM	= 6;
	public static final int MONTH_WEEK   = 7;
	public static final int FOREVER   = 8;
	public static final int YEAR   = 9;
	
	public static final long MINUTE_IN_MILLISEC = 300000l;
	public static final long HOUR_IN_MILLISEC = 300000*12l;
	public static final long DAY_IN_MILLISEC = 300000*12*24l;
	public static final long MONTH_IN_MILLISEC = 300000*12*24*30l;
	public static final long WEEK_IN_MILLISEC = 300000*12*24*7l;
	public static final Long YEAR_IN_MILLISEC = 300000*12*24*365l;
	
	public static final long MINIMUM_COUNT_FOR_POPULAR = 10;
	
	public static final String SEPRATOR_HOST = "@";
	public static final String SEPRATOR_CAT = ",";
	public static final String DEVICE_ID = "deviceID";
	
	public static final int INDIA			= 1;
	public static final int REST_OF_WORLD		= 0;
	public static final int ALL_LOCATION	= 2;
	public static final String ALL_GEOLOCATION_CITY	="All";
	
	public static final int RECENT_ARTICLE_LIMIT = 10;
	
	public static final int COOKIE_AGE = 7776000;
	
	public static final int COOKIE_AGE_WINDOW = 1296000;
	
	public static final String[] AGENCY = {"Reuters","PTI","AFP","TNN","AP","Mumbai Mirror",
									        "IANS","ANI","TOI Crest","Agencies","Bloomberg",
									        "ET Bureau","INDIATIMES MOVIES","OUR BUREAU",
									        "New York Times","TNN & Agencies","NYT News Service",
									        "Health Me Up","Men's Life Today","ET Now","BusinessWeek"};
	
	public static final Long[] APP_TYPE = {0l,1l,2l};
	public static final Long[] CONTENT_TYPE = {1l,2L,3L};
	
	
	public static enum SHARE {
		 FBSHARE, FBCLICK, FBLIKE, FBTOTAL, FBCOMMENT,TWTOTAL;
		}
	
	public static boolean isNonUTFHost(String host)
	{
		if(host.equalsIgnoreCase("navbharattimes.indiatimes.com")
				|| host.equalsIgnoreCase("vijaykarnataka.indiatimes.com")
				|| host.equalsIgnoreCase("maharashtratimes.indiatimes.com"))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public static final String DEFAULT_HOST = "timesofindia.indiatimes.com";
	public static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	//OEM Constants
	public static final String OEM_VENDOR_KEY = "OEMVendorList";

	
	public static final String DEFAULT_OEM_BRAND = "TOI";
	public static final String DEFAULT_MAKE = "ALL";

	public static final int THREE_HOUR_INTERVAL = 1;
	public static final int SIX_HOUR_INTERVAL = 2;
	public static final int NINE_HOUR_INTERVAL = 3;
	public static final int TWELVE_HOUR_INTERVAL = 4;
	public static final int DAY_INTERVAL = 5;
	public static final int WEEK_INTERVAL = 6;
	public static final int MONTH_INTERVAL = 7;
	public static final int YEAR_INTERVAL = 9;
	public static final int CUSTOM_INTERVAL = 8;
		
	//String secret = "$$ecret#or@riginalEquipment||\\//||anufacturer**";
	public static final String PRIVATE_KEY= "/opt/keys/pvt.der";
	
	public static String getStackTrace(Exception exception){
		Writer writer = new StringWriter();
		PrintWriter printWriter = new PrintWriter(writer);
		exception.printStackTrace(printWriter);
		String s = writer.toString();
		return s;
	}
	
	public static boolean chkNull(String text) {
		//System.out.println("Value : " + text);
		if(text!= null && !text.trim().equals("") && !text.equals("undefined")) {
			return false;
		}
		return true;
	}

	
	public static String removeSQLI(String text) {
		//System.out.println("Value : " + text);
		String update = text;
		if(text!= null && !text.trim().equals("") && !text.equals("undefined")) {
			update = text.replaceAll("[$]*", "");
		}
		return update;
	}
}
