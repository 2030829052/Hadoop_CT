/**
 * @Time : 2022/12/4 18:44
 * @Author : jin
 * @File : AnalysisTextTool.class
 */
package org.fengyue.analysis.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.fengyue.analysis.io.MySQLRedisTextOutPutFormat;
import org.fengyue.analysis.mapper.AnalysisTextMapper;
import org.fengyue.analysis.reducer.AnalysisTextReducer;
import org.fengyue.commom.constant.Names;


/**
 * 分析数据工具类
 */
public class AnalysisTextTool implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //job
        Job job = Job.getInstance();
        //jar包
        job.setJarByClass(AnalysisTextTool.class);
        //扫描
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getVal()));
        //mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getVal(),
                scan,
                AnalysisTextMapper.class,
                Text.class,
                Text.class,
                job
        );
        //reducer
        job.setReducerClass(AnalysisTextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //outPutFormat
        job.setOutputFormatClass(MySQLRedisTextOutPutFormat.class);

        boolean flg = job.waitForCompletion(true);
        if (flg) {
            return JobStatus.State.SUCCEEDED.getValue();
        } else {
            return JobStatus.State.FAILED.getValue();
        }

    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
