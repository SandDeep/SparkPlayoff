package com.poc.spark;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

public class JavaRecoverableNetworkWordCount {

	public static void main(String[] args) {
		final String ip = args[0];
		final int port = Integer.parseInt(args[1]);
		final String checkpointDirectory = args[2];
		final String outputPath = args[3];

		JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return createContext(ip, port, checkpointDirectory, outputPath);
			}
		};
		
		JavaStreamingContext ssc=JavaStreamingContext.getOrCreate(checkpointDirectory, contextFactory);
		ssc.start();
		ssc.awaitTermination();
	}
	
	private static JavaStreamingContext createContext(String ip, int port,
			String checkpointDirectory, String outputPath) {

		System.out.println("Creating new context");
		File outputFile = new File(outputPath);
		if (outputFile.exists()) {
			outputFile.delete();
		}

		SparkConf sparkConf = new SparkConf().setAppName("JavaRecoverableNetworkWordCount");
		// Create the context with a 1 second batch size
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,Durations.seconds(1));
		ssc.checkpoint(checkpointDirectory);
		
		//TODO
		return ssc;
	}
}
