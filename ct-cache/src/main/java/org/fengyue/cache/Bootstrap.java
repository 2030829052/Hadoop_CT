/**
 * @Time : 2022/12/5 14:00
 * @Author : jin
 * @File : Bootstrap.class
 */
package org.fengyue.cache;

import org.fengyue.commom.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 启动缓存客户端，向redis中增加缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {

        //读取mysql的数据

        Connection connection = JDBCUtil.getConnection();
        ;

        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();

        //存储用户-id映射关系
        String queryUserSQL = "select id,tel from ct_user";
        String queryDateSQL = "select id,year,month,day from ct_date";
        PreparedStatement pstat = null;
        ResultSet result = null;
        try {
            pstat = connection.prepareStatement(queryUserSQL);
            result = pstat.executeQuery();
            while (result.next()) {
                int id = result.getInt(1);
                String tel = result.getString(2);
                userMap.put(tel, id);
            }
            pstat = connection.prepareStatement(queryDateSQL);
            result = pstat.executeQuery();
            while (result.next()) {
                int id = result.getInt(1);
                String year = result.getString(2);
                String month = result.getString(3);
                String day = result.getString(4);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                if (day.length() == 1) {
                    day = "0" + day;
                }
                dateMap.put(year + month + day, id);
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (result != null) result.close();
                if (pstat != null) pstat.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        System.out.println(userMap.size());
//        System.out.println(dateMap.size());

        //向redis存储数据
        Jedis jedis = new Jedis("hadoop101", 6379);


        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user", key, "" + value);
        }

        Iterator<String> dataIterator = dateMap.keySet().iterator();
        while (dataIterator.hasNext()) {
            String key = dataIterator.next();
            Integer value = dateMap.get(key);
            jedis.hset("ct_date", key, "" + value);
        }

    }
}

