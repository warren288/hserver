package com.warrenyoung.hserver.hiveserver;

import com.warrenyoung.hserver.util.DBUtil;
import com.warrenyoung.hserver.util.HiveUtil;
import com.warrenyoung.hserver.util.SimpUtil;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@WebServlet(name = "HiveServlet")
public class HiveServlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String strName = request.getParameter("name");
        String strPwd = request.getParameter("password");
        String strSql = request.getParameter("sql");

        response.setContentType("text/html;charset=utf-8”");
        response. setCharacterEncoding("UTF-8");
        PrintWriter pw = response.getWriter();

        boolean isUser = DBUtil.isUser(strName, strPwd);
        if (! isUser){
            pw.write("用户不合法");
            pw.close();
            return;
        } else if (strSql.trim().length() == 0){
            pw.write("SQL语句不合法");
            pw.close();
            return;
        }

        HiveUtil.HiveStatementB state;
        try {
            state = HiveUtil.createStatement(HiveUtil.getHiveCon());
        } catch (SQLException e) {
            pw.write("连接Hive失败");
            pw.close();
            e.printStackTrace();
            return;
        }
        String[] lstSql = strSql.split(";");
        List<String> lstStr = new ArrayList<>();
        for (String sql : lstSql) {
            System.out.print(sql);
            try {
                if (sql.trim().toLowerCase().startsWith("select")) {
                    ResultSet rs = state.executeQuery(sql);
                    int colcnt = rs.getMetaData().getColumnCount();
                    System.out.print(colcnt);
                    while (rs.next()) {
                        List<String> row = new ArrayList<>();
                        for (int idx = 1; idx <= colcnt; idx++) {
                            row.add(rs.getObject(idx).toString());
                        }
                        lstStr.add(SimpUtil.combineStrList(row, "\t"));
                    }
                } else {
                    boolean b = state.execute(sql);
                    lstStr.add(String.format("SQL=%s 运行%s", sql, b ? "成功" : "失败"));
                }
            }catch(SQLException e){
                e.printStackTrace();
                lstStr.add(String.format("SQL=%s 运行%s", sql, "失败" ));
            }
        }
        pw.write(SimpUtil.combineStrList(lstStr, "\n"));
        pw.close();
    }
}
