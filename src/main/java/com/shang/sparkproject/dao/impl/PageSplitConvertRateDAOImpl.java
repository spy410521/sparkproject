package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IPageSplitConvertRateDAO;
import com.shang.sparkproject.domain.PageSplitConvertRate;
import com.shang.sparkproject.jdbc.JdbcHelper;

public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql="insert into page_split_convert_rate values(?,?)";

        Object[] params=new Object[]{
                pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JdbcHelper jdbcHelper=JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
