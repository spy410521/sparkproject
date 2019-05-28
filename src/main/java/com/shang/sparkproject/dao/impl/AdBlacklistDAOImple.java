package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IAdBlacklistDAO;
import com.shang.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdBlacklistDAOImple implements IAdBlacklistDAO {

    @Override
    public void insertBash(List<Long> useridList) {
        String sql="INSERT INTO ad_blacklist VALUES(?)";

        List<Object[]> paramsList=new ArrayList<>();

        for(long param:useridList){
            Object[] obj=new Object[]{param};
            paramsList.add(obj);
        }

        JdbcHelper jdbcHelper=JdbcHelper.getInstance();
        jdbcHelper.executeBash(sql,paramsList);
    }

    @Override
    public List<Long> findAll() {
        String sql="SELECT * FROM ad_blacklist";

        List<Long> useridList=new ArrayList<>();
        JdbcHelper jdbcHelper=JdbcHelper.getInstance();
        jdbcHelper.executeQuery(sql, null, new JdbcHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    long userid= rs.getLong(1);
                    useridList.add(userid);
                }
            }
        });

        return useridList;
    }


}
