package com.shang.sparkproject.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class FastJson {
    public static void main(String[] args) {
        String jsonArray = "[{'学生':'张三','年龄':'16'},{'学生':'李四','年龄':'13'}]";
        String jsonObj = "{'学生':'shang','年龄':'19'}";
        JSONArray jsonArray1 = JSONArray.parseArray(jsonArray);
        JSONObject obj1 = jsonArray1.getJSONObject(1);
        String json2Str = jsonArray1.toJSONString();
        System.out.println(obj1.getString("学生"));
        System.out.println(json2Str);


        Object json = JSON.parse(jsonObj);
        System.out.println(((JSONObject) json).getString("年龄"));
        System.out.println(((JSONObject) json).toJSONString());
    }
}
