package com.poc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache
 * /spark/examples/streaming/JavaNetworkWordCount.java
 * */
public class JavaNetworkWordCount {

	public static void main(String[] args) {
		// Create a local StreamingContext with two working thread and batch
		// interval of 1 second
		SparkConf conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]");

		// Duration - Batch Interval
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration(1));

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
				"192.168.34.52", 9999, StorageLevel.MEMORY_AND_DISK_SER());

		// jssc.fileStream(directory, kClass, vClass, fClass)
		// jssc.textFileStream(directory);
		// jssc.queueStream(queueofRDDs)
		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 4219862261504131741L;

					@Override
					public Iterable<String> call(String t) throws Exception {
						return Lists.newArrayList(t.split("\\s+"));
					}

				});

		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = -6401478827515043008L;

					@Override
					public Tuple2<String, Integer> call(String t)
							throws Exception {
						return new Tuple2<String, Integer>(t, 1);
					}
				});

		JavaPairDStream<String, Integer> wordCounts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 6042487904138513158L;

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}

				});

		wordCounts.print();

		/*************** Windowed Operation *********************/

		// Reduce function adding two integers, defined separately for clarity
		Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		};

		// Reduce last 30 seconds of data, every 10 seconds
		JavaPairDStream<String, Integer> windowedWordCounts = pairs
				.reduceByKeyAndWindow(reduceFunc, Durations.seconds(10),
						Durations.seconds(10));
		
		windowedWordCounts.print();
		
		/*************** Join Operations *********************/
		
		JavaPairDStream<String, String> stream1=null;
		JavaPairDStream<String, String> stream2=null;
		JavaPairDStream<String, Tuple2<String, String>> joinedStream=stream1.join(stream2);
		
		JavaPairDStream<String, String> windowedStream1 = stream1.window(Durations.seconds(20));
		JavaPairDStream<String, String> windowedStream2 = stream2.window(Durations.minutes(1));
		JavaPairDStream<String, Tuple2<String, String>> joinedStream1 = windowedStream1.join(windowedStream2);
		
		//Stream-dataset joins
		JavaPairRDD<String, String> dataset = null;
		JavaPairDStream<String, String> windowedStream = stream1.window(Durations.seconds(20));
		/*JavaPairDStream<String, String> joinedStream2 = windowedStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<U>>() {

			@Override
			public JavaRDD<U> call(JavaPairRDD<String, String> v1)
					throws Exception {
				return null;
			}
				});*/
		
		/*
		 * Note that when these lines are executed, Spark Streaming only sets up
		 * the computation it will perform after it is started, and no real
		 * processing has started yet. To start the processing after all the
		 * transformations have been setup, we finally call start method.
		 */
		jssc.start();
		jssc.awaitTermination();
		// jssc.stop(false);
		jssc.close();
	}
	
}
