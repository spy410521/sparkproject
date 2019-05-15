package com.shang.sparkproject.dao;

import com.shang.sparkproject.domain.Task;

public interface ItaskDao {
    public Task findbyId(long taskid);
}
