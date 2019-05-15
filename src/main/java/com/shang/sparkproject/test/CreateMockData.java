package com.shang.sparkproject.test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class CreateMockData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CreateMockData").setMaster("local[2]");
         JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext= new  SQLContext(sc);
        MockDataTest.mock(sc,sqlContext);
    }
}
