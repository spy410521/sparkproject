package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.*;

public class DaoFactory {

    public static ItaskDao getTaskDao(){
        return new TaskDaoImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrStatDAOImpl();
    }

    public static IgetSessionRandomExtractDAO getSessionRandomExtractDAO(){
        return  new getSessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO(){
        return  new SessionDetailDAOImple();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO(){
        return  new top10CategoryDAOImpl();
    }
}
