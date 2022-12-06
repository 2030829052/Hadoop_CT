/**
 * @Time : 2022/12/4 19:19
 * @Author : jin
 * @File : MySQLRedisTextOutPutFormat.class
 */
package org.fengyue.analysis.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.fengyue.commom.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;


/**
 * MySQL数据格式化输出对象
 */
public class MySQLRedisTextOutPutFormat extends OutputFormat<Text, Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {

        private Connection connection = null;
        private Jedis jedis = null;

        public MySQLRecordWriter() {
            // 获取资源
            connection = JDBCUtil.getConnection();
            jedis = new Jedis("hadoop101", 6379);
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

            String tel = jedis.hget("ct_user", ks[0]);
            String date = jedis.hget("ct_date", ks[1]);

            int tel_id = -1, date_id = -1;
            if (null != tel) tel_id = Integer.parseInt(tel);
            if (null != date) date_id = Integer.parseInt(date);

            PreparedStatement pstat = null;
            try {
                if (tel_id != -1 && date_id != -1) {
                    String insertSQL = "insert into calllog ( tel_id, date_id, sumcall, sumduration ) values ( ?, ?, ?, ? )";
                    pstat = connection.prepareStatement(insertSQL);


                    pstat.setInt(1, tel_id);
                    pstat.setInt(2, date_id);
                    pstat.setInt(3, sumCall);
                    pstat.setInt(4, sumDuration);
                    pstat.executeUpdate();
                }
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

            if (jedis != null) {
                jedis.close();
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
