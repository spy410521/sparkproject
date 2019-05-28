package com.shang.sparkproject.dao;

public interface IAdUserClickCountDAO {
    public int findClickCountByMultiKey(String date,Long userid,Long adid);
}
