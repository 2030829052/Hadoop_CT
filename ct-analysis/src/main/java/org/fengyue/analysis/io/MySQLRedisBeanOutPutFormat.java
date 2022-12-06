/**
 * @Time : 2022/12/4 19:19
 * @Author : jin
 * @File : MySQLRedisBeanOutPutFormat.class
 */
package org.fengyue.analysis.io;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.fengyue.analysis.kv.AnalysisKey;
import org.fengyue.analysis.kv.AnalysisValue;
import org.fengyue.commom.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;


/**
 * MySQL数据格式化输出对象
 */
public class MySQLRedisBeanOutPutFormat extends OutputFormat<AnalysisKey, AnalysisValue> {
    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue> {
        private Connection connection = null;
        private Jedis jedis = null;

        public MySQLRecordWriter() {
            //获取资源
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
        @Override
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {


            int sumCall = Integer.parseInt(value.getSumCall());
            int sumDuration = Integer.parseInt(value.getSumDuration());


            String tel = jedis.hget("ct_user", key.getTel());
            String date = jedis.hget("ct_date", key.getDate());

            int tel_id = -1, date_id = -1;
            if (tel != null) tel_id = Integer.parseInt(tel);
            if (date != null) date_id = Integer.parseInt(date);


            PreparedStatement pstat = null;
            try {
                if (tel_id != -1 && date_id != -1) {

                    String insertSQL = "insert into calllog (tel_id,date_id,sumcall,sumduration) values (?, ?, ? ,?)";
                    pstat = connection.prepareStatement(insertSQL);


                    pstat.setInt(1, tel_id);
                    pstat.setInt(2, date_id);
                    pstat.setInt(3, sumCall);
                    pstat.setInt(4, sumDuration);

                    pstat.executeUpdate();
                }

            } catch (Exception e) {
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
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (jedis != null) {
                try {
                    jedis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }


    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    private FileOutputCommitter committer = null;

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(taskAttemptContext);
            committer = new FileOutputCommitter(output, taskAttemptContext);
        }
        return committer;
    }
}