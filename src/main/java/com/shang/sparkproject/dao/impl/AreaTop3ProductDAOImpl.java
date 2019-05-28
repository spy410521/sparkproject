package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IAreaTop3ProductDAO;
import com.shang.sparkproject.domain.AreaTop3Product;
import com.shang.sparkproject.jdbc.JdbcHelper;

import java.util.ArrayList;
import java.util.List;

public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {
    @Override
    public void insertBash(List<AreaTop3Product> list) {
        String sql="INSERT INTO area_top3_product VALUES(?,?,?,?,?,?,?,?)";

        List<Object[]> paramsList=new ArrayList<>();

        for(AreaTop3Product areaTop3Product:list){
            Object[] params=new Object[8];

            params[0] = areaTop3Product.getTaskid();
            params[1] = areaTop3Product.getArea();
            params[2] = areaTop3Product.getAreaLevel();
            params[3] = areaTop3Product.getProductid();
            params[4] = areaTop3Product.getCityInfos();
            params[5] = areaTop3Product.getClickCount();
            params[6] = areaTop3Product.getProductName();
            params[7] = areaTop3Product.getProductStatus();

            paramsList.add(params);
        }

        JdbcHelper jdbcHelper=JdbcHelper.getInstance();
        jdbcHelper.executeBash(sql,paramsList);
    }
}
