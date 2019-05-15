package com.shang.sparkproject.test;

import com.shang.sparkproject.jdbc.JdbcHelper;
import org.apache.commons.collections.map.HashedMap;
import org.mortbay.util.StringMap;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class jdbc {
    public static void main(String[] args) {
//        insert();
//        insertBatch();
        query();

    }

    public static void insert() {
        JdbcHelper jdbc = JdbcHelper.getInstance();
        String sql = "insert into test_user values(?,?)";
        jdbc.executeUpdate(sql, new Object[]{"shang", 34});
    }

    public static void insertBatch() {
        JdbcHelper jdbc = JdbcHelper.getInstance();
        String sql = "insert into test_user values(?,?)";
        List<Object[]> list = new ArrayList<Object[]>();
        Object[] obj1 = new Object[]{"wang", 34};
        Object[] obj2 = new Object[]{"zhao", 12};
        list.add(obj1);
        list.add(obj2);
        jdbc.executeBash(sql, list);
    }



    public static void query() {
        final Map<String, Object> map = new HashMap<String, Object>();
        JdbcHelper jdbc = JdbcHelper.getInstance();
        String sql = "select * from test_user where age =?";
        jdbc.executeQuery(sql, new Object[]{34}, new JdbcHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    String name=rs.getString(1);
                    int    age=rs.getInt(2);
                    map.put(name,age);
                }
            }
        });
        for(Map.Entry<String,Object> ent:map.entrySet()){
            System.out.println("name:"+ent.getKey()+"age:"+ent.getValue());
        }

    }

}
