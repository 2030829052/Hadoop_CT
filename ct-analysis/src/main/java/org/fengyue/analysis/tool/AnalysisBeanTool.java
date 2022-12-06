/**
 * @Time : 2022/12/4 18:44
 * @Author : jin
 * @File : AnalysisTextTool.class
 */
package org.fengyue.analysis.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.fengyue.analysis.io.MySQLRedisBeanOutPutFormat;
import org.fengyue.analysis.kv.AnalysisKey;
import org.fengyue.analysis.kv.AnalysisValue;
import org.fengyue.analysis.mapper.AnalysisBeanMapper;
import org.fengyue.analysis.reducer.AnalysisBeanReducer;
import org.fengyue.commom.constant.Names;


/**
 * 分析数据工具类
 */
public class AnalysisBeanTool implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //job
        Job job = Job.getInstance();
        //jar包
        job.setJarByClass(AnalysisBeanTool.class);
        //扫描
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getVal()));
        //mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getVal(),
                scan,
                AnalysisBeanMapper.class,
                AnalysisKey.class,
                Text.class,
                job
        );
        //reducer
        job.setReducerClass(AnalysisBeanReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);
        //outPutFormat
        job.setOutputFormatClass(MySQLRedisBeanOutPutFormat.class);

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
