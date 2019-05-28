package com.shang.sparkproject.product;

import org.apache.spark.sql.api.java.UDF1;

public class RemoveRandomPrefixUDF implements UDF1<String,String> {
    @Override
    public String call(String s) throws Exception {
        String[] valSplit= s.split("_");
        return valSplit[1];

    }
}
