package com.shang.sparkproject.jdbc;

import com.shang.sparkproject.conf.ConfigurationManager;
import com.shang.sparkproject.constant.Constant;

import javax.security.auth.login.Configuration;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * jdbc辅助类
 */
public class JdbcHelper {

    //数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    static {
        try {
            String driver = ConfigurationManager.getValue(Constant.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //单例的实现
    private JdbcHelper() {
        int databaseSize = ConfigurationManager.getInteger(Constant.JDBC_DATABASE_SIZE);
        for (int i = 0; i <= databaseSize; i++) {
            String url = ConfigurationManager.getValue(Constant.JDBC_URL);
            String user = ConfigurationManager.getValue(Constant.JDBC_USER);
            String passwd = ConfigurationManager.getValue(Constant.JDBC_PASSWD);
            try {
                Connection conn = DriverManager.getConnection(url, user, passwd);
                datasource.add(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static volatile JdbcHelper instance = null;

    public static JdbcHelper getInstance() {
        if (instance == null) {
            synchronized (JdbcHelper.class) {
                if (instance == null) {
                    instance = new JdbcHelper();
                }
            }
        }
        return instance;
    }


    //提过数据库连接的方法
    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    //执行增删改方法
    public int executeUpdate(String sql, Object[] params) {
        int resInt = 0;
        Connection conn = null;
        PreparedStatement psmt = null;

        try {
            conn = getConnection();
            psmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                psmt.setObject(i + 1, params[i]);
            }
            resInt = psmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }

        return resInt;
    }

    //执行查询方法
    public void executeQuery(String sql, Object[] params, QueryCallBack callBack) {
        Connection conn = null;
        PreparedStatement psmt = null;

        try {
            conn = getConnection();
            psmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                psmt.setObject(i + 1, params[i]);
            }
            ResultSet rs = psmt.executeQuery();
            callBack.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }


    }

    /**
     * 查询回调接口
     */
    public interface QueryCallBack {
        void process(ResultSet rs) throws Exception;
    }


    //执行批量插入的方法
    public int[] executeBash(String sql, List<Object[]> paramsList) {
        int[] rst = null;
        Connection conn = null;
        PreparedStatement psmt = null;

        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            psmt = conn.prepareStatement(sql);
            for (Object[] objs : paramsList) {
                for (int i = 0; i < objs.length; i++) {
                    psmt.setObject(i + 1, objs[i]);
                }
                psmt.addBatch();
            }
            rst = psmt.executeBatch();

            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn!=null){
                datasource.push(conn);
            }
        }

        return null;
    }

}
