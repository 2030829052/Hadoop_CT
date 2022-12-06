/**
 * @Time : 2022/12/4 19:15
 * @Author : jin
 * @File : AnalysisTextReduce.class
 */
package org.fengyue.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.fengyue.analysis.kv.AnalysisKey;
import org.fengyue.analysis.kv.AnalysisValue;

import java.io.IOException;

/**
 * 分析数据库Reducer
 */
public class AnalysisBeanReducer extends Reducer<AnalysisKey, Text, AnalysisKey, AnalysisValue> {
    @Override
    protected void reduce(AnalysisKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumCall = 0;
        int sumDuration = 0;
        for (Text value : values) {
            sumCall++;
            sumDuration += Integer.parseInt(value.toString());
        }
        context.write(key, new AnalysisValue(""+sumCall,""+sumDuration));
    }

}
