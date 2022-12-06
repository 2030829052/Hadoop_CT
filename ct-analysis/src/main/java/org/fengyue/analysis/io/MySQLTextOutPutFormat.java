/**
 * @Time : 2022/12/4 19:19
 * @Author : jin
 * @File : MySQLTextOutPutFormat.class
 */
package org.fengyue.analysis.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.fengyue.commom.util.JDBCUtil;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * Mysql的数据格式化输出对象
 */
public class MySQLTextOutPutFormat extends OutputFormat<Text, Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {

        private Connection connection = null;
        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();

        public MySQLRecordWriter() {
            // 获取资源
            connection = JDBCUtil.getConnection();
            PreparedStatement pstat = null;
            ResultSet rs = null;

            try {

                String queryUserSql = "select id, tel from ct_user";
                pstat = connection.prepareStatement(queryUserSql);
                rs = pstat.executeQuery();
                while (rs.next()) {
                    Integer id = rs.getInt(1);
                    String tel = rs.getString(2);
                    userMap.put(tel, id);
                }

                rs.close();

                String queryDateSql = "select id, year, month, day from ct_date";
                pstat = connection.prepareStatement(queryDateSql);
                rs = pstat.executeQuery();
                while (rs.next()) {
                    Integer id = rs.getInt(1);
                    String year = rs.getString(2);
                    String month = rs.getString(3);
                    if (month.length() == 1) {
                        month = "0" + month;
                    }
                    String day = rs.getString(4);
                    if (day.length() == 1) {
                        day = "0" + day;
                    }
                    dateMap.put(year + month + day, id);
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if (pstat != null) {
                    try {
                        pstat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 输出数据
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(Text key, Text value) throws IOException, InterruptedException {

            String[] values = value.toString().split("_");
            int sumCall = Integer.parseInt(values[0]);
            int sumDuration = Integer.parseInt(values[1]);

            String[] ks = key.toString().split("_");

            String tel = ks[0];
            String date = ks[1];


            int tel_id = userMap.get(tel);
            int date_id = dateMap.get(date);

            PreparedStatement pstat = null;
            try {
                String insertSQL = "insert into calllog ( tel_id, date_id, sumcall, sumduration ) values ( ?, ?, ?, ? )";
                pstat = connection.prepareStatement(insertSQL);


                pstat.setInt(1, tel_id);
                pstat.setInt(2, date_id);
                pstat.setInt(3, sumCall);
                pstat.setInt(4, sumDuration);
                pstat.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (pstat != null) {
                    try {
                        pstat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 释放资源
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}

