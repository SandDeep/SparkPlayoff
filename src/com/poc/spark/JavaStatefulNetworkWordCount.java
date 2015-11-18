package com.poc.spark;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.base.Optional;

/**
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache
 * /spark/examples/streaming/JavaStatefulNetworkWordCount.java
 * 
 * @author Deepesh.Maheshwari
 *
 */
public class JavaStatefulNetworkWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		final Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

			@Override
			public Optional<Integer> call(List<Integer> values,
					Optional<Integer> state) throws Exception {
				int newSum = state.or(0);
				for (Integer value : values) {
					newSum += value;
				}
				return Optional.of(newSum);
			}
		};

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaStatefulNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(1));
		ssc.checkpoint(".");

		// Initial RDD input to updateStateByKey
		List<Tuple2<String, Integer>> tuples = Arrays.asList(
				new Tuple2<String, Integer>("hello", 1),
				new Tuple2<String, Integer>("world", 1));

		JavaPairRDD<String, Integer> initalRDD = ssc.sc().parallelizePairs(
				tuples);
	}
}
