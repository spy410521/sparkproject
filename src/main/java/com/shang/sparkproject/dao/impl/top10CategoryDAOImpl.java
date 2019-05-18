package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.ITop10CategoryDAO;
import com.shang.sparkproject.domain.Top10Category;
import com.shang.sparkproject.jdbc.JdbcHelper;

public class top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category category) {
        String sql="insert into top10_category values(?,?,?,?,?)";
        Object[] params=new Object[]{category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JdbcHelper jdbcHelper=JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
