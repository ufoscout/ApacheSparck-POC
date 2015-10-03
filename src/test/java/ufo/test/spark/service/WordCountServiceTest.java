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

import static org.junit.Assert.*;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import ufo.test.spark.SparkApplicationTests;

public class WordCountServiceTest extends SparkApplicationTests {

    @Autowired
    private JavaSparkContext sparkContext;

	@Test
	public void should_count_words_from_file_on_local_fs() {

		Map<String, Integer> wordsCount = WordCountService.count(sparkContext, "./resources/MotorMattMakesGood.txt", "./target/MotorMattMakesGood_count.txt");

		assertNotNull(wordsCount);
		assertFalse(wordsCount.isEmpty());

		System.out.println(wordsCount);

	}

}
