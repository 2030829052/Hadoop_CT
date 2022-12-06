/**
 * @Time : 2022/12/4 18:42
 * @Author : jin
 * @File : AnalysisData.class
 */
package org.fengyue.analysis;

import org.apache.hadoop.util.ToolRunner;
import org.fengyue.analysis.tool.AnalysisBeanTool;

/**
 * 分析数据
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception {
        //int result = ToolRunner.run(new AnalysisTextTool(), args);
        int result = ToolRunner.run(new AnalysisBeanTool(), args);
    }
}
