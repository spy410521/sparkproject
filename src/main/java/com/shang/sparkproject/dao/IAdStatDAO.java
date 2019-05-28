package com.shang.sparkproject.dao;

import com.shang.sparkproject.domain.AdStat;

import java.util.List;

public interface IAdStatDAO {
    void insertOrUpdateBash(List<AdStat> adStats);
}
