/**
 * @Time : 2022/12/4 0:35
 * @Author : jin
 * @File : CalllogConsumer.class
 */
package org.fengyue.consumer.bean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.fengyue.commom.bean.Consumer;
import org.fengyue.consumer.dao.HbaseDao;
import org.fengyue.commom.constant.Names;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志消费者对象
 */
public class CalllogConsumer implements Consumer {
    /**
     * 消费数据
     */
    @Override
    public void consume() {

        try {
            //创建配置对象
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            //获取flume采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getVal()));
            //Hbase数据访问对象
            HbaseDao dao = new HbaseDao();
            //初始化
            dao.init();

            //消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    //Hbase插入数据
                    dao.insertData(consumerRecord.value());
//                    Calllog calllog = new Calllog(consumerRecord.value());
//                    dao.insertData(calllog);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
