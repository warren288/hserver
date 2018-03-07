package com.warrenyoung.hserver.util;

import java.sql.*;
import java.util.List;
import java.util.UUID;

import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.jdbc.logs.InPlaceUpdateStream;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;

/**
 * 操作Hive的工具类
 *
 *
 * 需要添加hive的jdbc包作为依赖，可从 <HIVE_HOME>/jdbc 目录下获取，
 */
public class HiveUtil {

    public static Connection getHiveCon(){
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = null;
        try {
           con = DriverManager.getConnection("jdbc:hive2://localhost:10000/blog;auth=noSasl", "warrenyoung", "warrenyoung");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }

    public static HiveStatementB createStatement(Connection con) throws SQLException {
        return new HiveStatementB((HiveStatement)con.createStatement());
    }

    public static class HiveStatementB {
        private HiveStatement state;
        private Thread thread;
        public HiveStatementB(HiveStatement state) {
            this.state = state;

            thread = new Thread(){
                @Override
                public void run() {
                    try {
                        while (!state.isClosed() && state.hasMoreLogs()) {
                            try {
                                for (String log : state.getQueryLog(true, 100)) {
                                    System.out.println(log);
                                }
                                sleep(500L);
                            } catch (SQLException e) {
                                e.printStackTrace();
                                return;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                return;
                            }
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            };

        }

        public boolean execute(String sql) throws SQLException {
            // todo 添加历史记录，查询耗时
//            thread.start();
//            String uuid = UUID.randomUUID().toString();
            return state.execute(sql);
        }

        public ResultSet executeQuery(String sql) throws SQLException{
            // todo 添加历史记录
//            thread.start();
//            String uuid = UUID.randomUUID().toString();
            return state.executeQuery(sql);
        }
    }
}
