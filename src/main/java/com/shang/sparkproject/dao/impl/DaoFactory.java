package com.shang.sparkproject.dao.impl;

import com.shang.sparkproject.dao.*;

public class DaoFactory {

    public static ItaskDao getTaskDao() {
        return new TaskDaoImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static IgetSessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new getSessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAOImple();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new top10CategoryDAOImpl();
    }

    public static ITop10SessionDAO getTop10SessionDAO() {
        return new Top10SessionDAOImpl();
    }

    public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
        return new PageSplitConvertRateDAOImpl();
    }

    public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
        return new AreaTop3ProductDAOImpl();
    }

    public static IAdStatDAO getAdStatDAO(){
        return new AdStatDAOImpl();
    }

    public static IAdUserClickCountDAO getAdUserClickCountDAO(){
        return new AdUserClickCountDAOImpl();
    }

    public static IAdBlacklistDAO getAdBlacklistDAO(){
        return new AdBlacklistDAOImple();
    }

    public static IAdProvinceTop3DAO getAdProvinceTop3DAO(){
        return  new AdProvinceTop3DAOImpl();
    }

    public  static IAdClickTrendDAO getIAdClickTrendDAO(){
        return  new IAdClickTrendDAOImple();
    }
}
