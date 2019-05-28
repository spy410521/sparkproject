package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.IAdStatDAO;
import com.shang.sparkproject.domain.AdStat;
import com.shang.sparkproject.domain.AdStatQueryResult;
import com.shang.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdStatDAOImpl implements IAdStatDAO {
    @Override
    public void insertOrUpdateBash(List<AdStat> adStats) {
        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        // 区分开来哪些是要插入的，哪些是要更新的
        List<AdStat> insertAdStats = new ArrayList<>();
        List<AdStat> updateAdStats = new ArrayList<>();

        String selectSQL = "SELECT count(*) "
                + "FROM ad_stat "
                + "WHERE date=? "
                + "AND province=? "
                + "AND city=? "
                + "AND ad_id=?";

        for (AdStat adStat : adStats) {
            AdStatQueryResult adStatQueryResult = new AdStatQueryResult();

            Object[] params = new Object[]{adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()
            };
            jdbcHelper.executeQuery(selectSQL, params, new JdbcHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    while (rs.next()) {
                        int count = rs.getInt(1);
                        adStatQueryResult.setCount(count);
                    }
                }
            });

            if (adStatQueryResult.getCount() > 0) {
                updateAdStats.add(adStat);
            } else {
                insertAdStats.add(adStat);
            }
        }

        // 对于需要插入的数据，执行批量插入操作
        String insertSql = "INSERT INTO ad_stat VALUES(?,?,?,?,?)";

        List<Object[]> insertParams = new ArrayList<>();

        for (AdStat adStat : insertAdStats) {
            Object[] obj = new Object[]{adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid(),
                    adStat.getClickCount()
            };
            insertParams.add(obj);
        }

        jdbcHelper.executeBash(insertSql,insertParams);

        // 对于需要更新的数据，执行批量更新操作
        String updateSql="UPDATE ad_stat SET click_count=click_count+? "
                + "FROM ad_stat "
                + "WHERE date=? "
                + "AND province=? "
                + "AND city=? "
                + "AND ad_id=?";
        List<Object[]> updateParams=new ArrayList<>();
        for(AdStat adStat:updateAdStats){
            Object[] obj=new Object[]{adStat.getClickCount(),
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()
            };
            updateParams.add(obj);
        }
        jdbcHelper.executeBash(updateSql,updateParams);

    }
}
