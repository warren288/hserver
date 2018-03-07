package com.warrenyoung.hserver.util;

import com.warrenyoung.hserver.bean.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DBUtil {

    public static Connection getMysqlCon(){
        Connection con = null;
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/hserver";
        String user = "root";
        String password = "2880111019";
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }



    public static List<User> getUserList(){
        List<User> listUser = new ArrayList<>();
        Connection con = getMysqlCon();
        try {
            Statement statement = con.createStatement();
            String sql = "select * from `user`";
            ResultSet rs = statement.executeQuery(sql);
            while(rs.next()){
                String name = rs.getString("name");
                String password = rs.getString("password");
                listUser.add(new User(name, password));
            }
            rs.close();
            con.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }finally{
            System.out.println("数据库数据成功获取！！");
        }
        return listUser;
    }

    public static boolean isUser(String name, String password){
        boolean is = false;
        try {
            Connection con = getMysqlCon();
            Statement statement = con.createStatement();
            String sql = String.format("select * from `user` where name='%s' and password='%s' ", name, password);
            ResultSet rs = statement.executeQuery(sql);
            while(rs.next()){
                is = true;
            }
            rs.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return is;
    }



}
