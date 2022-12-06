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
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.fengyue.commom.util.JDBCUtil;

import java.sql.Connection;


/**
 * MySQL数据格式化输出对象
 */
public class MySQLOutPutFormat extends OutputFormat<Text, Text> {
    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {
        private Connection connection = null;

        public MySQLRecordWriter() {
            //获取资源
            connection = JDBCUtil.getConnection();
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
        public void write(Text key, Text value) throws IOException, InterruptedException {
            String[] info = value.toString().split("_");
            int sumCall = Integer.parseInt(info[0]);
            int sumDuration = Integer.parseInt(info[1]);

            PreparedStatement pstat = null;
            try {

                String insertSQL = "insert into calllog (tel_id,date_id,sumcall,sumduration) values (?, ?, ? ,?)";
                pstat = connection.prepareStatement(insertSQL);

                pstat.setInt(1, 2);
                pstat.setInt(2, 3);
                pstat.setInt(3, sumCall);
                pstat.setInt(4, sumDuration);

                pstat.executeUpdate();

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
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }


    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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