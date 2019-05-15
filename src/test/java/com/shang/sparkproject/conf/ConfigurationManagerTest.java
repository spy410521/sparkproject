package com.shang.sparkproject.conf;

import static org.junit.Assert.*;

public class ConfigurationManagerTest {

    @org.junit.Test
    public void getValue() {
        System.out.println(ConfigurationManager.getValue("testkey1"));

    }
}