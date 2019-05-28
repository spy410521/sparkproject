package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.ITop10SessionDAO;
import com.shang.sparkproject.domain.Top10Session;
import com.shang.sparkproject.jdbc.JdbcHelper;

public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{
                top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()
        };

        JdbcHelper jdbcHelper=JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
