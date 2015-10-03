/*******************************************************************************
 * Copyright 2015 Francesco Cina'
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package ufo.test.spark.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import ufo.test.spark.SparkApplicationTests;

public class WordCountServiceTest extends SparkApplicationTests {

	public final static String INPUT_FILE_PATH = "./resources/MotorMattMakesGood.txt";

	@Autowired
	private JavaSparkContext sparkContext;

	@Test
	public void should_count_words_from_file_on_local_fs() {
		Map<String, Integer> wordsCount = WordCountService.wordsCountMap(sparkContext, INPUT_FILE_PATH);
		assertNotNull(wordsCount);
		assertFalse(wordsCount.isEmpty());
	}

	@Test
	public void should_count_words_from_file_on_local_fs_and_save_in_a_file() throws IOException {
		String outputFilePath = "./target/MotorMattMakesGood_count.txt";
		Path outputPath = Paths.get(outputFilePath);
		deleteRecursively(outputPath);
		WordCountService.wordsCountMapToFile(sparkContext, INPUT_FILE_PATH, outputFilePath);
		assertTrue(Files.exists(Paths.get(outputFilePath)));
	}

	@Test
	public void should_return_10_most_used_words() {
		int returnWords = 10;
		Map<String, Integer> wordsCount = WordCountService.getMostUsedWords(sparkContext, INPUT_FILE_PATH, returnWords);

		assertEquals(returnWords, wordsCount.size());

		System.out.println("Printing most used words");
		wordsCount.forEach((key, value) -> {
			System.out.println("word [" + key + "] is used " + value + " times");
		});

		assertTrue(wordsCount.containsKey("the"));
		assertTrue(wordsCount.containsKey("a"));

	}

	@Test
	public void should_return_10_longest_words() {
		int returnWords = 10;
		Map<String, Integer> wordsCount = WordCountService.getLongestWords(sparkContext, INPUT_FILE_PATH, returnWords);

		assertEquals(returnWords, wordsCount.size());

		System.out.println("Printing most used words");
		wordsCount.forEach((key, value) -> {
			System.out.println("word [" + key + "] is used " + value + " times");
		});

		assertTrue(wordsCount.containsKey("www.gutenberg.org/contact"));

	}

	private void deleteRecursively(Path path) throws IOException {
		if (Files.exists(path)) {
			//delete recursively
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}
			});
		}
		assertFalse(Files.exists(path));
	}
}
