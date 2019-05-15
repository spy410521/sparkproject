package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IgetSessionRandomExtractDAO;
import com.shang.sparkproject.domain.SessionRandomExtract;
import com.shang.sparkproject.jdbc.JdbcHelper;

public class getSessionRandomExtractDAOImpl implements IgetSessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql="insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params=new Object[]{sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};
        JdbcHelper.getInstance().executeUpdate(sql,params);

    }
}
