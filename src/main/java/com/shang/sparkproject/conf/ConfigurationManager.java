package com.shang.sparkproject.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 */
public class ConfigurationManager {
    private static Properties properties = new Properties();

    static {
        try {
            InputStream inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static  String getValue(String key){
        return properties.getProperty(key);
    }

    public static Integer getInteger(String key){
        String value=properties.getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static Boolean getBoolean(String key){
        String value=properties.getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public static long getLong(String key){
        String value=properties.getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }



}
