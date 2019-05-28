package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IAdClickTrendDAO;
import com.shang.sparkproject.domain.AdClickTrend;
import com.shang.sparkproject.domain.AdClickTrendQueryResult;
import com.shang.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class IAdClickTrendDAOImple  implements IAdClickTrendDAO {
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrends) {

        JdbcHelper jdbcHelper=JdbcHelper.getInstance();

        List<AdClickTrend> updateList=new ArrayList<>();
        List<AdClickTrend> insertList=new ArrayList<>();

        String querySQL="select count(*) from ad_click_trend where "
                + "WHERE date=? "
                + "AND hour=? "
                + "AND minute=? "
                + "AND ad_id=?";


        for(AdClickTrend adClickTrend:adClickTrends){
            final AdClickTrendQueryResult queryResult = new AdClickTrendQueryResult();
            Object[] params=new Object[]{adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdid()
            };

            jdbcHelper.executeQuery(querySQL, params, new JdbcHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                        while(rs.next()){
                            int count= rs.getInt(1);
                            queryResult.setCount(count);
                        }
                }
            });

            if(queryResult.getCount()>0){
                updateList.add(adClickTrend);
            }else {
                insertList.add(adClickTrend);
            }
        }

        //更新操作
        String upateSql="update ad_click_trend set click_count=click_count+?"
                + "WHERE date=? "
                + "AND hour=? "
                + "AND minute=? "
                + "AND ad_id=?";

        List<Object[]> updateParamsList=new ArrayList<>();
        for(AdClickTrend adClickTrend:updateList){
            Object[] obj=new Object[5];
            obj[0]=adClickTrend.getClickCount();
            obj[1]=adClickTrend.getDate();
            obj[2]=adClickTrend.getHour();
            obj[3]=adClickTrend.getMinute();
            obj[4]=adClickTrend.getAdid();

            updateParamsList.add(obj);
        }

        jdbcHelper.executeBash(upateSql,updateParamsList);


        //插入操作
        String insertSql="insert into ad_click_trend(?,?,?,?,?)";
        List<Object[]> insertParamsList=new ArrayList<>();
        for(AdClickTrend adClickTrend:insertList){
            Object[] obj=new Object[5];
            obj[0]=adClickTrend.getDate();
            obj[1]=adClickTrend.getHour();
            obj[2]=adClickTrend.getMinute();
            obj[3]=adClickTrend.getAdid();
            obj[4]=adClickTrend.getClickCount();

            insertParamsList.add(obj);
        }

        jdbcHelper.executeBash(insertSql,insertParamsList);

    }
}
