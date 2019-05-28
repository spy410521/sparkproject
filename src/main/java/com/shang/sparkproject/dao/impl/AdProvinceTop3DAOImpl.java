package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IAdProvinceTop3DAO;
import com.shang.sparkproject.domain.AdProvinceTop3;
import com.shang.sparkproject.jdbc.JdbcHelper;

import java.util.ArrayList;
import java.util.List;

public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {
    @Override
    public void insertBash(List<AdProvinceTop3> list) {
        JdbcHelper jdbcHelper=JdbcHelper.getInstance();

        //去重
        List<String> dateProvinces=new ArrayList<>();
        for(AdProvinceTop3 adp:list){
            String date= adp.getDate();
            String province=adp.getProvince();
            String key=date+"_"+province;
            if(!dateProvinces.contains(key)){
                dateProvinces.add(key);
            }
        }

        //根据去重后的date和province，进行批量删除操作
        String deletSQL="DELETE FROM ad_province_top3 WHERE date=? AND province=?";
        List<Object[]> paramsList=new ArrayList<>();
        for(String dateProvince:dateProvinces){
            String[] dateProvinceSplits=dateProvince.split("_");
            String  date=dateProvinceSplits[0];
            String  province=dateProvinceSplits[1];
            Object[] param=new Object[]{date,province};
            paramsList.add(param);
        }
        jdbcHelper.executeBash(deletSQL,paramsList);


        // 批量插入传入进来的所有数据
        String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";
        List<Object[]> insertParamsList=new ArrayList<>();
        for(AdProvinceTop3 adProvinceTop3:list){
            String date=adProvinceTop3.getDate();
            String province=adProvinceTop3.getProvince();
            long adid=Long.valueOf(adProvinceTop3.getAdid());
            long clickCount=Long.valueOf(adProvinceTop3.getClickCount());

            Object[] params=new Object[]{date,province,adid,clickCount};
            insertParamsList.add(params);
        }

        jdbcHelper.executeBash(insertSQL,insertParamsList);

    }
}
