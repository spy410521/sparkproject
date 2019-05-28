package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IAdUserClickCountDAO;
import com.shang.sparkproject.domain.AdUserClickCountQueryResult;
import com.shang.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {
    @Override
    public int findClickCountByMultiKey(String date, Long userid, Long adid) {
        final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

        String sql="SELECT click_count "
                + "FROM ad_user_click_count "
                + "WHERE date=? "
                + "AND user_id=? "
                + "AND ad_id=?";

        Object[] params=new Object[]{date,userid,adid};

        JdbcHelper jdbcHelper=JdbcHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    int clickCount= rs.getInt(1);
                    queryResult.setClickCount(clickCount);
                }
            }
        });

        return queryResult.getClickCount();
    }
}
