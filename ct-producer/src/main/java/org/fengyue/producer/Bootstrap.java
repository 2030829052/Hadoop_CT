/**
 * @Time : 2022/12/3 12:36
 * @Author : jin
 * @File : Bootstrap.class
 */
package org.fengyue.producer;

import org.fengyue.commom.bean.Producer;
import org.fengyue.producer.bean.LocalFileProducer;
import org.fengyue.producer.io.LocalFileDataIn;
import org.fengyue.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.out.println("系统参数错误，请按照指定格式传递！");
            System.exit(1);
        }
        //构建生产者对象
        Producer producer = new LocalFileProducer();
//        producer.setIn(new LocalFileDataIn("E:\\IDEA hadoop项目\\Hadoop_CT\\Data\\In\\contact.log"));
//        producer.setOut(new LocalFileDataOut("E:\\IDEA hadoop项目\\Hadoop_CT\\Data\\Out\\call.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));
        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
