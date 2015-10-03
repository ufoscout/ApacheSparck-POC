/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ufo.test.spark.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 *
 * @author ufo
 */
public class WordCountService {

	private static final Pattern SPACE = Pattern.compile(" ");

	/**
	 *
	 * @param sc
	 * @param inputFilePath
	 * @param outputFilePath
	 * @return a map with all the text words and how much time they appear
	 */
	public static Map<String, Integer> wordsCountMap(JavaSparkContext sc, String inputFilePath) {
		JavaPairRDD<String, Integer> counts = reduce(sc, inputFilePath);
		return counts.collectAsMap();
	}

	/**
	 * Creates a map with all the text words and how much time they appear and save it to file
	 * @param sc
	 * @param inputFilePath
	 * @param outputFilePath
	 * @return
	 */
	public static void wordsCountMapToFile(JavaSparkContext sc, String inputFilePath, String outputFilePath) {
		JavaPairRDD<String, Integer> counts = reduce(sc, inputFilePath);
		counts.saveAsTextFile(outputFilePath);
	}

	/**
	 *
	 * @param sc
	 * @param inputFilePath
	 * @param wordsToReturn
	 * @return a map with the most used words
	 */
	public static Map<String, Integer> getMostUsedWords(JavaSparkContext sc, String inputFilePath, final int wordsToReturn) {
		JavaPairRDD<String, Integer> counts = reduce(sc, inputFilePath);

		Map<String, Integer> result = new LinkedHashMap<>();

		counts.takeOrdered(wordsToReturn, new TupleComparatorStringInt())
				.forEach(tuple -> result.put(tuple._1, tuple._2));

		return result;
	}

	/**
	 *
	 * @param sc
	 * @param inputFilePath
	 * @param wordsToReturn
	 * @return a map with the longest words
	 */
	public static Map<String, Integer> getLongestWords(JavaSparkContext sc, String inputFilePath, final int wordsToReturn) {
		JavaPairRDD<String, Integer> counts = reduce(sc, inputFilePath);

		Map<String, Integer> result = new LinkedHashMap<>();

		counts.sortByKey(new StringComparator(), false)
		.take(wordsToReturn)
		.forEach(tuple -> result.put(tuple._1, tuple._2));

		return result;
	}

	private static JavaPairRDD<String, Integer> reduce(JavaSparkContext sc, String inputFilePath) {

		JavaRDD<String> rdd = sc.textFile(inputFilePath);

		return rdd.flatMap(x -> Arrays.asList(SPACE.split(x)))
				.mapToPair(x -> new Tuple2<String, Integer>(x, 1))
				.reduceByKey((x, y) -> x + y);

	}

	public static class TupleComparatorStringInt implements Comparator<Tuple2<String,Integer>>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Tuple2<String,Integer> o1, Tuple2<String,Integer> o2) {
			return o2._2.compareTo(o1._2);
		}
	}

	public static class StringComparator implements Comparator<String>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(String o1, String o2) {
			return Integer.compare(o1.length(), o2.length());
		}
	}

}
