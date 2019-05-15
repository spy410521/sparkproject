package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.ItaskDao;
import com.shang.sparkproject.domain.Task;
import com.shang.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;

public class TaskDaoImpl implements ItaskDao {
    @Override
    public Task findbyId(long taskid) {

        final Task task=new Task();
        String sql="select * from task where task_id=?";
        JdbcHelper jdbc=JdbcHelper.getInstance();
        Object[] obj=new Object[]{taskid};
        jdbc.executeQuery(sql, obj, new JdbcHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()){
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }
}
