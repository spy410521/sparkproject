package com.shang.sparkproject.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

public class RandomPrefixUDF implements UDF2<String, Integer, String> {
    @Override
    public String call(String val, Integer num) throws Exception {
        Random random = new Random();
        int randomNum = random.nextInt(num);
        return randomNum + "_" + val;
    }
}
