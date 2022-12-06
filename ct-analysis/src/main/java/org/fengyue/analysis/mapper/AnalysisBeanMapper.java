/**
 * @Time : 2022/12/4 19:11
 * @Author : jin
 * @File : AnalysisTextMapper.class
 */
package org.fengyue.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.fengyue.analysis.kv.AnalysisKey;

import java.io.IOException;

/**
 * 分析数据的mapper
 */
public class AnalysisBeanMapper extends TableMapper<AnalysisKey, Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //
        String rowkey = Bytes.toString(key.get());

        String[] info = rowkey.split("_");

        String call1 = info[1];
        String call2 = info[3];
        String calltime = info[2];
        String duration = info[4];

        String year = calltime.substring(0, 4);
        String month = calltime.substring(0, 6);
        String day = calltime.substring(0, 8);


        //主叫用户
        //年
        context.write(new AnalysisKey(call1, year), new Text(duration));
        //月
        context.write(new AnalysisKey(call1, month), new Text(duration));
        //日
        context.write(new AnalysisKey(call1, day), new Text(duration));


        //被叫用户
        context.write(new AnalysisKey(call2, year), new Text(duration));
        context.write(new AnalysisKey(call2, month), new Text(duration));
        context.write(new AnalysisKey(call2, day), new Text(duration));
    }
}
