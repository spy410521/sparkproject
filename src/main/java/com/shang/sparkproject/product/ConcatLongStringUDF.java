package com.shang.sparkproject.product;

import org.apache.spark.sql.api.java.UDF3;

public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {

    @Override
    public String call(Long s, String s2, String split) throws Exception {
        return Long.valueOf(s) + split + s2;
    }
}
