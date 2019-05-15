package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.ISessionDetailDAO;
import com.shang.sparkproject.domain.SessionDetail;
import com.shang.sparkproject.jdbc.JdbcHelper;

public class SessionDetailDAOImple implements ISessionDetailDAO {
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] objs = new Object[]{
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};
        JdbcHelper.getInstance().executeUpdate(sql,objs);
    }
}
