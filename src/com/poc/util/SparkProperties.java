package com.poc.util;

public final class SparkProperties {

	public static String AUTO_OFFSET_RESET_EARLIEST = "smallest";
	public static String AUTO_OFFSET_RESET_LATEST = "largest";
	public static String SPARK_MASTER = "spark://iBeatMongoDB3382:7077";
	public static String LOCAL_SPARK_MASTER = "local[2]";
	public static String APPLICATION_NAME = "Spark Application";
	public static String EXECUTOR_MEMORY = "1g";
	
	//Mongo Output Properties
	public static String MONGO_OUTPUT_URI = "mongodb://192.168.24.204:27017/iBeat.timedCount";
	public static String LOCAL_MONGO_OUTPUT_URI = "mongodb://localhost:27017/iBeat.timedCount";
	public static String MONGO_IP = "localhost";
	public static String MONGO_PORT = "27017";

}
