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
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 分析数据的mapper
 */
public class AnalysisTextMapper extends TableMapper<Text, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {


        String rowkey = Bytes.toString(key.get());

        String[] info = rowkey.split("_");

        String call1 = info[1];
        String call2 = info[3];
        String calltime = info[2];
        String duration = info[4];

        String year = calltime.substring(0, 4) + "0000";
        String month = calltime.substring(0, 6) + "0";
        String day = calltime.substring(0, 8);


        //主叫用户
        //年
        context.write(new Text(call1 + "_" + year), new Text(duration));
        //月
        context.write(new Text(call1 + "_" + month), new Text(duration));
        //日
        context.write(new Text(call1 + "_" + day), new Text(duration));


        //被叫用户
        context.write(new Text(call2 + "_" + year), new Text(duration));
        context.write(new Text(call2 + "_" + month), new Text(duration));
        context.write(new Text(call2 + "_" + day), new Text(duration));
    }
}
