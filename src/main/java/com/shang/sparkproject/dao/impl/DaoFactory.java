package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.ISessionAggrStatDAO;
import com.shang.sparkproject.dao.ItaskDao;

public class DaoFactory {

    public static ItaskDao getTaskDao(){
        return new TaskDaoImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrStatDAOImpl();
    }
}
