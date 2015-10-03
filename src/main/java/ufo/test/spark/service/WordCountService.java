/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ufo.test.spark.service;

import java.util.Arrays;
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

	public static Map<String, Integer> count(JavaSparkContext sc, String inputFilePath, String outputFilePath) {

		JavaRDD<String> rdd = sc.textFile(inputFilePath);

		JavaPairRDD<String, Integer> counts = rdd
				.flatMap(x -> Arrays.asList(SPACE.split(x)))
				.mapToPair(x -> new Tuple2<String, Integer>(x, 1))
				.reduceByKey((x, y) -> x + y);

		try {
			counts.saveAsTextFile(outputFilePath);
		} catch (Exception e) {
			// if the file already exists it fails. We don't care here.
		}
		return counts.collectAsMap();
	}

}
