/**
 * @Time : 2022/12/4 19:55
 * @Author : jin
 * @File : JDBCUtil.class
 */
package org.fengyue.commom.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class JDBCUtil {
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://hadoop101:3306/Hadoop-CT?useUnicode=true&useSSL=false&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "123456";

    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
