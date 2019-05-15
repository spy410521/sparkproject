package com.shang.sparkproject.test;

import com.shang.sparkproject.dao.ItaskDao;
import com.shang.sparkproject.dao.impl.DaoFactory;
import com.shang.sparkproject.domain.Task;

public class TaskDao {
    public  static void main(String[] args){
        ItaskDao itaskDao=DaoFactory.getTaskDao();
        Task task= itaskDao.findbyId(1);
        System.out.println(task.getTaskid()+":"+task.getTaskName()+":"+task.getCreateTime());

    }
}
