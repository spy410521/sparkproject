package com.shang.sparkproject.dao;

import java.util.List;

public interface IAdBlacklistDAO {
    void insertBash(List<Long> useridList);

    /**
     * 查询所有广告黑名单用户
     * @return
     */
    List<Long> findAll();
}
