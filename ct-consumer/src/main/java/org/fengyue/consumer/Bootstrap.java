/**
 * @Time : 2022/12/4 0:32
 * @Author : jin
 * @File : Bootstarp.class
 */
package org.fengyue.consumer;

import org.fengyue.commom.bean.Consumer;
import org.fengyue.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * 启动类
 * 使用Kafka消费者获取Flume采集的数据
 * 将数据存储到Hbase
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        //创建消费者
        Consumer consumer = new CalllogConsumer();
        //消费数据
        consumer.consume();
        //关闭资源
        consumer.close();

    }
}
