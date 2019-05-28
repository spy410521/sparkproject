package com.shang.sparkproject.dao;

import com.shang.sparkproject.domain.AdClickTrend;

import java.util.List;

/**
 * 广告点击趋势DAO接口
 * @author Administrator
 *
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
