package com.warrenyoung.hserver.hiveserver;

import com.warrenyoung.hserver.bean.User;
import com.warrenyoung.hserver.util.DBUtil;
import com.warrenyoung.hserver.util.SimpUtil;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

@javax.servlet.annotation.WebServlet(name = "Servlet")
public class DemoServlet extends javax.servlet.http.HttpServlet {
    protected void doPost(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws javax.servlet.ServletException, IOException {
    }

    protected void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws javax.servlet.ServletException, IOException {
        response.setContentType("text/html");
        PrintWriter pw = response.getWriter();
        List<User> listUser =  DBUtil.getUserList();

        pw.write(SimpUtil.combine(listUser, "</p>"));
    }
}
