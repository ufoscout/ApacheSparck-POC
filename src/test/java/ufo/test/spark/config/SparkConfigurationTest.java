/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ufo.test.spark.config;

import static org.junit.Assert.assertNotNull;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import ufo.test.spark.SparkApplicationTests;

/**
 *
 * @author ufo
 */
public class SparkConfigurationTest extends SparkApplicationTests {

    @Autowired
    private JavaSparkContext sparkContext;

    @Test
    public void spark_context_should_be_defined() {
        assertNotNull(sparkContext);
    }

}
