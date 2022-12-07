可视化地址：https://github.com/2030829052/Hadoop-CT-Web


### Hadoop实训（中国电信客服）

# 

项目背景

​		通信运营商每时每刻会产生大量的通信数据，例如通话记录，短信记录，彩信记录，第三方服务资费等等繁多信息。数据量如此巨大，除了要满足用户的实时查询和展示之外，还需要定时定期的对已有数据进行离线的分析处理。例如，当日话单，月度话单，季度话单，年度话单，通话详情，通话记录等等+。我们以此为背景，寻找一个切入点，学习其中的方法论。当前我们的需求是：统计每天、每月以及每年的每个人的通话次数及时长。

#### 开发环境

系统：Window11专业版  ContOS7

IDE：IDEA2022.1.4专业版	Maven4.0	JDK1.8

集群环境：

​	Hadoop	3.3.0

​	Zookeeper	3.6.3

​	Hbase	2.4.10

​	Flume	1.9.0

​	Kafka	2.12

​	Redis	5.0.6 

硬件：

​	hadoop101 内存：4GB 	处理器：4 	硬盘：50GB

​	hadoop102 内存：4GB 	处理器：4 	硬盘：50GB

​	hadoop103 内存：4GB 	处理器：4 	硬盘：50GB



#### 业务流程

![image-20221203201754440](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203201754440.png)



#### 一.数据生产

##### 1.1业务流程

![image-20221203201917352](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203201917352.png)

1.1创建Maven工程



创建项目Hadoop_CT  创建项目子模块：ct-common  	ct-producer

![image-20221203113844047](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203113844047.png)

##### 1.2创建基础对象接口

###### 1.2.1值对象接口Value

```java
package org.fengyue.ct_web.commom.bean;

/**
 * 值对象接口
 */

public interface Value {
    public void setVal(Object val);

    public Object getVal();
}

```

###### 1.2.2生产者接口Produce

```java
package org.fengyue.ct_web.commom.bean;

import java.io.Closeable;

/**
 * 生产者接口
 */

public interface Producer extends Closeable {
    public void setIn(DataIn in);

    public void setOut(DataOut out);

    /**
     * 生产数据
     */
    public void produce();
}

```

###### 1.2.3数据接口 DataIn DataOut

```java
package org.fengyue.ct_web.commom.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 数据来源
 */
public interface DataIn extends Closeable {
    public void setPath(String path);

    public Object read() throws IOException;

    public <T extends Data> List<T> read(Class<T> clazz) throws IOException;
}

```

```java
package org.fengyue.ct_web.commom.bean;

import java.io.Closeable;

/**
 * 数据出口
 */
public interface DataOut extends Closeable {
    public void setPath(String path);

    public void write(Object data) throws Exception;

    public void write(String data) throws Exception;
}

```

###### 1.2.4名称枚举类	Names

```java
package org.fengyue.ct_web.commom.constant;

import Value;

/**
 * 名称常量枚举类
 */
public enum Names implements Value {
    NAMESPACE("ct");
    private String name;

    private Names(String name) {
        this.name = name;
    }


    @Override
    public String val() {
        return name;
    }
}
```

##### 1.3创建生产者对象

###### 1.3.1创建生产者对象类

```java
/**
 * @Time : 2022/12/3 12:38
 * @Author : jin
 * @File : LocalFileProducer.class
 */
package org.fengyue.ct_web.producer.bean;

import DataIn;
import DataOut;
import Producer;

import java.io.IOException;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    @Override
    public void setIn(DataIn in) {

    }

    @Override
    public void setOut(DataOut out) {

    }

    @Override
    public void produce() {

    }

    @Override
    public void close() throws IOException {

    }
}

```

###### 1.3.2创建本地文件类

```java
/**
 * @Time : 2022/12/3 12:50
 * @Author : jin
 * @File : LocalFileDataIn.class
 */
package org.fengyue.ct_web.producer.io;

import Data;
import DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地文件输入
 */
public class LocalFileDataIn implements DataIn {

    private BufferedReader reader = null;

    public LocalFileDataIn(String path) {
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Object read() throws IOException {
        return null;
    }

    /**
     * 读取数据返回数据集合
     *
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    @Override
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {

        List<T> ts = new ArrayList<>();
        try {
            //从数据文件中读取所有的数据
            String line = null;
            while ((line = reader.readLine()) != null) {
                //将数据转换为指定类型的对象，封装为集合返回
                T t = clazz.newInstance();
                t.setVal(line);
                ts.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return ts;
    }

    @Override
    public void close() throws IOException {

    }
}

```

```java
/**
 * @Time : 2022/12/3 12:51
 * @Author : jin
 * @File : LocalFileDataOut.class
 */
package org.fengyue.ct_web.producer.io;

import DataOut;

import java.io.*;

/**
 * 本地文件输出
 */
public class LocalFileDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFileDataOut(String path) {
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void write(Object data) throws Exception {
        write(data.toString());
    }

    /**
     * 将数据字符串生成到文件中
     *
     * @param data
     * @throws Exception
     */

    @Override
    public void write(String data) throws Exception {

        writer.println(data);
        //把流数据直接放在文件中
        writer.flush();
    }

    /**
     * 释放资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}

```

###### 1.3.3创建启动类

```java
/**
 * @Time : 2022/12/3 12:36
 * @Author : jin
 * @File : Bootstrap.class
 */
package org.fengyue.ct_web.producer;

import Producer;
import LocalFileProducer;

import java.io.IOException;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        //构建生产者对象
        Producer producer = new LocalFileProducer();
        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
```

##### 1.4获取通讯录数据

###### 1.4.1创建联系人对象类

```java
/**
 * @Time : 2022/12/3 13:15
 * @Author : jin
 * @File : Contact.class
 */
package org.fengyue.ct_web.producer.bean;

import Data;

/**
 * 联系人
 */
public class Contact extends Data {
    private String tel;
    private String name;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setVal(Object val) {
        content = (String) val;
        String[] values = content.split("\t");
        setName(values[1]);
        setTel(values[0]);
    }

    public String toString() {
        return "Contact[" + tel + ":" + name + "]";
    }
}
```

###### 1.4.2获取通讯录数据

```java
public void produce() {

    try {
        //读取通讯录数据
        List<Contact> contacts = in.read(Contact.class);
        for (Contact contact : contacts) {
            System.out.println(contact);
        }

       // while (flg) {
            //从通讯录随机查找电话号码（主叫，被叫）
            //生成随机通话时间
            //生成通话时长
            //生成通话记录
            //保存通话记录
        //}
    } catch (Exception e) {
        e.printStackTrace();
    }
}
```

![image-20221203143412517](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203143412517.png)

###### 1.4.3随机生成主被叫电话号码

![image-20221203150817895](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203150817895.png)





###### 1.4.4通话记录格式化类

```java
/**
 * @Time : 2022/12/3 15:13
 * @Author : jin
 * @File : NumberUtil.class
 */
package org.fengyue.ct_web.commom.util;

import java.text.DecimalFormat;

/**
 * 数字工具类
 */
public class NumberUtil {
    /**
     * 将数字格式化为字符串
     *
     * @param num
     * @param length
     * @return
     */
    public static String format(int num, int length) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 1; i <= length; i++) {
            stringBuilder.append("0");
        }
        DecimalFormat df = new DecimalFormat(stringBuilder.toString());
        return df.format(num);
    }
}


```

```java
/**
 * @Time : 2022/12/3 15:32
 * @Author : jin
 * @File : DateUtil.class
 */
package org.fengyue.ct_web.commom.util;

import Data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    /**
     * 日期对象格式化
     *
     * @param dateString
     * @param format
     * @return
     */
    public static Date parse(String dateString, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date;

    }

    public static String format(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

}
```

###### 1.4.5生成通话记录

![image-20221203154739140](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203154739140.png)

###### 1.4.6创建通话记录对象类

```java
/**
 * @Time : 2022/12/3 15:43
 * @Author : jin
 * @File : Calllog.class
 */
package org.fengyue.ct_web.producer.bean;

public class Calllog {
    private String call1;
    private String call2;
    private String calltime;
    private String duration;

    public Calllog(String call1, String call2, String calltime, String duration) {
        this.call1 = call1;
        this.call2 = call2;
        this.calltime = calltime;
        this.duration = duration;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String toString() {
        return call1 + "\t" + call2 + "\t" + calltime + "\t" + duration;
    }
}
```

###### 1.4.7保存通话记录

![image-20221203163641077](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203163641077.png)

![image-20221203163824344](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203163824344.png)

##### 1.5将项目打包到服务器

###### 1.5.1修改启动类

```java
/**
 * @Time : 2022/12/3 12:36
 * @Author : jin
 * @File : Bootstrap.class
 */
package org.fengyue.ct_web.producer;

import Producer;
import LocalFileProducer;
import LocalFileDataIn;
import LocalFileDataOut;

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
```

###### 1.5.2上传测试

![image-20221203173007625](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203173008890.png)

#### 二.数据消费

**利用Flume和Kafka将收集不断生产的数据，并且将数据插入到HBase中**

##### 2.1业务流程

![image-20221204152716675](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204152716675.png)

##### 2.2配置Kafka

###### 2.2.1下载解压kafka

![image-20221203181042189](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203181042189.png)

###### 2.2.2配置server.properties	

1. broker.id标识本机
2. log.dirs是kafka接收消息存放路径
3. zookeeper.connect指定连接的zookeeper集群地址

###### 2.2.3分发到集群

xsync kafka212

![image-20221203201200956](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203201200956.png)

###### 2.2.4配置集群启动命令

```shell
#! /bin/bash
case $1 in
        "start" ){
        for i in hadoop101 hadoop102 hadoop103;
        do
                echo "==============$i start=============="
                ssh $i "/opt/module/kafka212/bin/kafka-server-start.sh -daemon /opt/module/kafka212/config/server.properties"
        done
};;
        "stop" ){
        for i in hadoop101 hadoop102 hadoop103;
        do
                echo "==============$i stop=============="
                ssh $i "/opt/module/kafka212/bin/kafka-server-stop.sh"
        done
};;
esac
```

###### 2.2.5测试

![image-20221203201004801](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203201004801.png)

##### 2.3初步使用Kafka

###### 2.3.1创建主题



 kafka-topics.sh --zookeeper hadoop101:9092 --create --topic ct --partitions 3 --replication-factor 2

![image-20221203233751625](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221203233751625.png)

###### 2.3.2Kafka开始消费

kafka-console-consumer.sh --bootstrap-server hadoop101:9092 -topic ct（命令更新）

![image-20221204002016402](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204002016402.png)

###### 2.3.3编写flume脚本

```shell
# define
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# source
a1.sources.r1.type = exec
a1.sources.r1.command = tail -F -c +0 /opt/bigData/call.log
a1.sources.r1.shell = /bin/bash -c

# sink
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.kafka.bootstrap.servers = hadoop101:9092,hadoop102:9092,hadoop103:9092
a1.sinks.k1.kafka.topic = ct
a1.sinks.k1.kafka.flumeBatchSize = 20
a1.sinks.k1.kafka.producer.acks = 1
a1.sinks.k1.kafka.producer.linger.ms = 1

# channel
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# bind
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1




```

###### 2.3.4启动flume脚本

![image-20221204002328528](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204002328528.png)

![image-20221204001719972](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204001719972.png)

##### 2.4创建Kafka JavaAPI

###### 2.4.1创建消费者对象

```java
package org.fengyue.ct_web.commom.bean;

import java.io.Closeable;

/**
 * 消费者类
 */
public interface Consumer extends Closeable {

    /**
     * 消费数据
     */
    public void consume();
}
```

###### 2.4.2创建配置文件

```properties
bootstrap.servers=hadoop101:9092,hadoop102:9092,hadoop103:9092
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
group.id=fengyue
enable.auto.commit=true
auto.commit.interval.ms=1000
```

###### 2.4.3创建消费者对象

```java
/**
 * @Time : 2022/12/4 0:35
 * @Author : jin
 * @File : CalllogConsumer.class
 */
package org.fengyue.ct_web.consumer.bean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import Consumer;
import Names;

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
            //消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
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
```

###### 2.4.4创建启动类

```java
/**
 * @Time : 2022/12/4 0:32
 * @Author : jin
 * @File : Bootstarp.class
 */
package org.fengyue.ct_web.consumer;

import Consumer;
import CalllogConsumer;

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
```

###### 2.4.5测试API

![image-20221204010031100](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204010031866.png)

##### 2.5API结合Hbase

###### 2.5.1创建dao层

```java
/**
 * @Time : 2022/12/4 1:05
 * @Author : jin
 * @File : HbaseDao.class
 */
package org.fengyue.ct_web.consumer.dao;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import BaseDao;
import Names;
import ValueConstant;

/**
 * Hbase数据访问对象
 */
public class HbaseDao extends BaseDao {
    /**
     * 初始化
     */
    public void init() throws Exception {

    }

    /**
     * 插入数据
     *
     * @param value
     */
    public void insertData(String value) throws Exception {

        //3.保存数据
        putData();
    }
}
```

###### 2.5.2创建基础数据访问对象

```java
/**
 * @Time : 2022/12/4 1:03
 * @Author : jin
 * @File : BaseDao.class
 */
package org.fengyue.ct_web.commom.bean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import Names;
import ValueConstant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws IOException {
        getConnection();
        getAdmin();

    }

    protected void end() throws IOException {
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }
        Connection coon = getConnection();
        if (coon != null) {
            coon.close();
            connHolder.remove();
        }
    }

    /**
     * 创建表
     * 如果表存在则删除，删除后创建新表
     */
    protected void createTableXX(String name, Integer regionCount, String... families) throws Exception {

        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        if (admin.tableExists(tableName)) {
            //表存在删除表
            deleteTable(name);
        }
        //创建表
        createTable(name, regionCount, families);
    }

    protected void createTableXX(String name, String... families) throws Exception {

        createTableXX(name, null, families);
    }

    /**
     * 增加数据
     */
    protected void putData(String name, Put put) throws IOException {

        //获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        //增加数据
        table.put(put);
        //关闭表
        table.close();
    }

    /**
     * 删除表方法
     *
     * @param name
     * @throws Exception
     */
    protected void deleteTable(String name) throws Exception {
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 创建表
     *
     * @param name
     * @param families
     * @throws Exception
     */
    private void createTable(String name, Integer regionCount, String... families) throws Exception {

        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        if (families == null || families.length == 0) {
            families = new String[1];
            families[0] = Names.CF_INFO.getVal();
        }
        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }
        //增加预分区
        //分区键
        if (regionCount == null || regionCount <= 1) {
            admin.createTable(tableDescriptor);
        } else {
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }
    }

    /**
     * 计算分区号
     *
     * @return
     */
    protected int genRegionNum(String tel, String date) {
        String usercode = tel.substring(tel.length() - 4);
        String yearMonth = date.substring(0, 6);
        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        //ccr检验采用异或算法
        int ccr = Math.abs(userCodeHash ^ yearMonthHash);
        //取模
        int regionNum = ccr % ValueConstant.REGION_COUNT;

        return regionNum;
    }


    /**
     * 生成分区键
     *
     * @return
     */
    private byte[][] genSplitKeys(int regionCount) {

        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitkey = i + "|";
            bsList.add(Bytes.toBytes(splitkey));
        }
        bsList.toArray(bs);
        return bs;

    }

    /**
     * 创建命名空间
     * 如果存在不需要创建否则创建新的
     *
     * @param namespace
     */
    protected void createNamespaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {

            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 获取管理对象
     *
     * @return
     */
    protected synchronized Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 获取连接对象
     *
     * @return
     */
    protected Connection getConnection() throws IOException {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }

}
```

###### 2.5.3在Hbase中创建对应的表格

```java
public void init() throws Exception {
    start();

    createNamespaceNX(Names.NAMESPACE.getVal());
    createTableXX(Names.TABLE.getVal(), ValueConstant.REGION_COUNT, Names.CF_CALLER.getVal());

    end();
}
```

###### 2.5.4设置两个列族和rowkey

```java
public void insertData(String value) throws Exception {
    //将通话体制保存到Hbase中
    //1.获取通话日志数据
    String[] values = value.split("\t");
    String call1 = values[0];
    String call2 = values[1];
    String calltime = values[2];
    String duration = values[3];

    //2.创建数据对象
    /*
      rowkey设计
      rowkey应该具备唯一性
      最大值64KB 推荐长度10~100byte 最好是8的倍数
      散列原则：盐值散列：不能使用时间戳 在rowkey前增加随机数
              字符串反转：电话号码
              计算分区号：hashMap
     */
    String rowkey = genRegionNum(call1, calltime) + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration;
    Put put = new Put(Bytes.toBytes(rowkey));
    byte[] family = Bytes.toBytes(Names.CF_CALLER.getVal());

    put.addColumn(family, Bytes.toBytes("call1"), Bytes.toBytes(call1));
    put.addColumn(family, Bytes.toBytes("call2"), Bytes.toBytes(call2));
    put.addColumn(family, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
    put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(duration));
    //3.保存数据
    putData(Names.TABLE.getVal(), put);
}
```

###### 2.5.5测试

![image-20221204033517301](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204033517301.png)

![image-20221204033529156](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204033529156.png)

##### 2.5.6封装Hbase对象

###### 2.5.6.1创建api

```java
package org.fengyue.ct_web.commom.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TableRef {
    String value();
}
```

```java
package org.fengyue.ct_web.commom.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;


@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RowKey {

}
```

```java
package org.fengyue.ct_web.commom.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String family() default "info";

    String column() default "";
}

```

###### 2.5.6.2创建通话日志类

```java
/**
 * @Time : 2022/12/4 13:25
 * @Author : jin
 * @File : Calllog.class
 */
package org.fengyue.ct_web.consumer.bean;

import Column;
import RowKey;
import TableRef;

/**
 * 通话日志
 */
@TableRef("ct:calllog")
public class Calllog {
    @RowKey
    private String rowkey;
    @Column(family = "caller")
    private String call1;
    @Column(family = "caller")
    private String call2;
    @Column(family = "caller")
    private String calltime;
    @Column(family = "caller")
    private String duration;


    public Calllog() {

    }


    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public Calllog(String data) {
        String[] values = data.split("\t");
        call1 = values[0];
        call2 = values[1];
        calltime = values[2];
        duration = values[3];
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }


}
```

###### 2.5.6.3重写添加方法

```java
/**
 * 增加对象自动封装对象，将数据对象直接保存在hbase中
 *
 * @param obj
 */
protected void putData(Object obj) throws Exception {

    //反射
    Class clazz = obj.getClass();
    TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
    String tableName = tableRef.value();

    Field[] fs = clazz.getDeclaredFields();


    String stringRowKey = "";
    for (Field f : fs) {
        f.setAccessible(true);
        RowKey rowKey = f.getAnnotation(RowKey.class);
        if (rowKey != null) {
            stringRowKey = (String) f.get(obj);
            break;
        }
    }
    //获取表对象
    Connection conn = getConnection();
    Table table = conn.getTable(TableName.valueOf(tableName));
    Put put = new Put(Bytes.toBytes(stringRowKey));

    for (Field f : fs) {
        f.setAccessible(true);
        Column column = f.getAnnotation(Column.class);
        if (column != null) {
            String family = column.family();
            String colName = column.column();
            if (colName == null || "".equals(colName)) {
                colName = f.getName();
            }
            String value = (String) f.get(obj);
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
        }
    }

    //增加数据
    table.put(put);
    //关闭表
    table.close();

}
```



###### 2.5.6.4将数据封装为对象

```java
/**
 * 插入对象
 *
 * @param log
 * @throws Exception
 */
public void insertData(Calllog log) throws Exception {
    log.setRowkey(genRegionNum(log.getCall1(), log.getCalltime()) + "_" + log.getCall1() + "_" + log.getCalltime()
            + "_" + log.getCall2() + "_" + log.getDuration());

    putData(log);
}
```

2.5.6.5测试

![image-20221204144042806](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204144042806.png)

##### 2.6设置过滤

###### 2.6.1获取月份区间

```java
/**
 * 获取查询时的startrow,stoprow集合
 *
 * @return
 */
protected static List<String[]> getStartStopRowKeys(String tel, String start, String end) {
    List<String[]> rowkeyss = new ArrayList<String[]>();
    String startTime = start.substring(0, 6);
    String stopTime = end.substring(0, 6);

    Calendar startCal = Calendar.getInstance();
    startCal.setTime(DateUtil.parse(startTime, "yyyyMM"));

    Calendar stopCal = Calendar.getInstance();
    stopCal.setTime(DateUtil.parse(stopTime, "yyyyMM"));

    while (startCal.getTimeInMillis() <= stopCal.getTimeInMillis()) {
        //当前时间
        String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");

        int regionNum = genRegionNum(tel, nowTime);

        String startRow = regionNum + "_" + tel + "_" + nowTime;
        String stopRow = startRow + "|";
        String[] rowkeys = {startRow, stopRow};
        rowkeyss.add(rowkeys);

        //月份+1
        startCal.add(Calendar.MONTH, 1);
    }
    return rowkeyss;
}
```

测试

![image-20221204152018498](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204152018498.png)



##### 2.7协处理开发

###### 2.7.1区分主叫被叫

```java
public void insertData(String value) throws Exception {
    //将通话体制保存到Hbase中
    //1.获取通话日志数据
    String[] values = value.split("\t");
    String call1 = values[0];
    String call2 = values[1];
    String calltime = values[2];
    String duration = values[3];

    //2.创建数据对象
    /*
      rowkey设计
      rowkey应该具备唯一性
      最大值64KB 推荐长度10~100byte 最好是8的倍数
      散列原则：盐值散列：不能使用时间戳 在rowkey前增加随机数
              字符串反转：电话号码
              计算分区号：hashMap
     */
    //主叫用户
    String rowkey = genRegionNum(call1, calltime) + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";
    Put put = new Put(Bytes.toBytes(rowkey));
    byte[] family = Bytes.toBytes(Names.CF_CALLER.getVal());

    put.addColumn(family, Bytes.toBytes("call1"), Bytes.toBytes(call1));
    put.addColumn(family, Bytes.toBytes("call2"), Bytes.toBytes(call2));
    put.addColumn(family, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
    put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(duration));
    put.addColumn(family, Bytes.toBytes("flg"), Bytes.toBytes("1"));

    //被叫用户
    String calleeRowkey = genRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";
    Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
    byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getVal());

    calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
    calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
    calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
    calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
    calleePut.addColumn(calleeFamily, Bytes.toBytes("flg"), Bytes.toBytes("0"));

    //3.保存数据

    List<Put> puts = new ArrayList<Put>();
    puts.add(put);
    puts.add(calleePut);
    putData(Names.TABLE.getVal(), puts);
}
```

测试

![image-20221204155142027](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204155142027.png)





###### 2.7.2创建协处理器类

```java
/**
 * @Time : 2022/12/4 16:05
 * @Author : jin
 * @File : InsertCalleeCoprocessor.class
 */
package org.fengyue.ct_web.consumer.coprocessor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.client.Put;
import BaseDao;
import Names;

import java.io.IOException;
import java.util.Optional;

/**
 * 使用协处理器保存被叫用户功能
 */

public class InsertCalleeCoprocessor implements RegionObserver, RegionCoprocessor {
    //使用RegionObserver需要重写RegionCoprocessor中的getRegionObserver方法
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of((RegionObserver) this);
    }

    /**
     * 方法命名规则
     * login
     * logout
     * prePut
     * doPut：模板方法设计模型
     * 存在父子类；
     * 父类搭建算法骨架
     * 1.tel取用户代码 2.取时间年月 3.异或算分区 4.hash散列
     * do1.tel取后四位 do2.202212 do3. ^ do4. %
     */

    /**
     * 保存主叫用户数据之后由Hbase自动保存被叫用户数据
     *
     * @param e
     * @param put
     * @param edit
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit) throws IOException {

        //获取表
        Table table = e.getEnvironment().getConnection().getTable(TableName.valueOf(Names.TABLE.getVal()));
        //主叫用户的rowkey
        String rowkey = Bytes.toString(put.getRow());

        //被叫信息
        String[] info = rowkey.split("_");
        String call1 = info[1];
        String call2 = info[3];
        String calltime = info[2];
        String duration = info[4];
        String flg = info[5];
        CoprocessorDao coprocessorDao = new CoprocessorDao();

        //区分主被叫
        if ("1".equals(flg)) {
            //只有主叫用户保存才会触发

            String calleeRowkey = coprocessorDao.genRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";
            Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getVal());
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("flg"), Bytes.toBytes("0"));
            //保存
            table.put(calleePut);

        }
        //关闭
        table.close();
    }

    private class CoprocessorDao extends BaseDao {
        @Override
        public int genRegionNum(String tel, String date) {
            return super.genRegionNum(tel, date);
        }
    }

}
```

##### 2.8让表知道协议处理器

###### 2.8.1 BaseDao/createTable

```java
private void createTable(String name, String coprocessorClass, Integer regionCount, String... families) throws Exception {

    TableName tableName = TableName.valueOf(name);
    Admin admin = getAdmin();

    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    if (families == null || families.length == 0) {
        families = new String[1];
        families[0] = Names.CF_INFO.getVal();
    }
    for (String family : families) {
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
        tableDescriptor.addFamily(columnDescriptor);
    }
    //增加协议处理器
    if (coprocessorClass != null && !"".equals(coprocessorClass)) {
        tableDescriptor.addCoprocessor(coprocessorClass);
    }

    //增加预分区
    if (regionCount == null || regionCount <= 1) {
        admin.createTable(tableDescriptor);
    } else {
        //分区键
        byte[][] splitKeys = genSplitKeys(regionCount);
        admin.createTable(tableDescriptor, splitKeys);
    }
}
```

###### 2.8.2 BaseDao/createTableXX

```java
protected void createTableXX(String name, String coprocessorClass, Integer regionCount, String... families) throws Exception {

    Admin admin = getAdmin();
    TableName tableName = TableName.valueOf(name);

    if (admin.tableExists(tableName)) {
        //表存在删除表
        deleteTable(name);
    }
    //创建表
    createTable(name, coprocessorClass, regionCount, families);
}
```

###### 2.8.3 BaseDao/createTableXX

```java
protected void createTableXX(String name, String... families) throws Exception {

    createTableXX(name, null, null, families);
}
```

###### 2.8.4 HbaseDao/init

```java
public void init() throws Exception {
    start();

    createNamespaceNX(Names.NAMESPACE.getVal());
    createTableXX(Names.TABLE.getVal(), "InsertCalleeCoprocessor",
            ValueConstant.REGION_COUNT, Names.CF_CALLER.getVal(), Names.CF_CALLEE.getVal());

    end();
}
```

##### 2.9打包上传测试

![image-20221204172019743](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204172019743.png)

###### 2.9.1分发jar包

![image-20221204172234714](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204172234714.png)

###### 2.9.2测试

![image-20221204172557305](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204172557305.png)

![image-20221204172615040](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204172615040.png)

#### 三.数据分析

##### 3.1业务流程

![image-20221204152851500](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204152851500.png)

##### 3.2MySQL表设计

业务指标：

a) 用户每天主叫通话个数统计，通话时间统计。

b) 用户每月通话记录统计，通话时间统计。

c) 用户之间亲密关系统计。（通话次数与通话时间体现用户亲密关系）

![image-20221204183946496](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204183946496.png)

##### 3.3数据计算-启动类

```java
/**
 * @Time : 2022/12/4 18:42
 * @Author : jin
 * @File : AnalysisData.class
 */
package org.fengyue.ct_web.analysis;

import org.apache.hadoop.util.ToolRunner;
import AnalysisTextTool;

/**
 * 分析数据
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new AnalysisTextTool(), args);
    }
}
```

##### 3.2数据计算-Mapper

```java
/**
 * @Time : 2022/12/4 19:11
 * @Author : jin
 * @File : AnalysisTextMapper.class
 */
package org.fengyue.ct_web.analysis.mapper;

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

        //
        String rowkey = Bytes.toString(key.get());

        String[] info = rowkey.split("_");

        String call1 = info[1];
        String call2 = info[3];
        String calltime = info[2];
        String duration = info[4];

        String year = calltime.substring(0, 4) + "0000";
        String month = calltime.substring(0, 6) + "00";
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
```

##### 3.3数据计算-Reducer

```java
/**
 * @Time : 2022/12/4 19:15
 * @Author : jin
 * @File : AnalysisTextReduce.class
 */
package org.fengyue.ct_web.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 分析数据库Reducer
 */
public class AnalysisTextReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumCall = 0;
        int sumDuration = 0;
        for (Text value : values) {
            sumCall++;
            sumDuration += Integer.parseInt(value.toString());
        }
        context.write(key, new Text(sumCall + "_" + sumDuration));
    }

}
```

##### 3.4数据计算-OutPutFormat

```java
/**
 * @Time : 2022/12/4 19:19
 * @Author : jin
 * @File : MySQLTextOutPutFormat.class
 */
package org.fengyue.ct_web.analysis.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import JDBCUtil;

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

                String insertSQL = "insert into calllog (tel_id,date,sumcall,sumduration) values (?, ?, ? ,?)";
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
```

##### 3.5数据计算-工具类

```java
/**
 * @Time : 2022/12/4 18:44
 * @Author : jin
 * @File : AnalysisTextTool.class
 */
package org.fengyue.ct_web.analysis.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import MySQLTextOutPutFormat;
import AnalysisTextMapper;
import AnalysisTextReducer;
import Names;


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
        job.setOutputFormatClass(MySQLTextOutPutFormat.class);

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
```



##### 3.6初步打包测试

**注意！避免版本冲突需要将打包好文件中的Hdfs Jar包删除并将MySQL jar包上传到hadoop依赖文件夹中！！！**

![image-20221204204122554](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204204124383.png)



![image-20221204204136290](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221204204136290.png)

##### 3.7在OutPutFormat中获取用户和时间数据

```java
/**
 * @Time : 2022/12/4 19:19
 * @Author : jin
 * @File : MySQLTextOutPutFormat.class
 */
package org.fengyue.ct_web.analysis.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import JDBCUtil;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * Mysql的数据格式化输出对象
 */
public class MySQLTextOutPutFormat extends OutputFormat<Text, Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {

        private Connection connection = null;
        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();

        public MySQLRecordWriter() {
            // 获取资源
            connection = JDBCUtil.getConnection();
            PreparedStatement pstat = null;
            ResultSet rs = null;

            try {

                String queryUserSql = "select id, tel from ct_user";
                pstat = connection.prepareStatement(queryUserSql);
                rs = pstat.executeQuery();
                while (rs.next()) {
                    Integer id = rs.getInt(1);
                    String tel = rs.getString(2);
                    userMap.put(tel, id);
                }

                rs.close();

                String queryDateSql = "select id, year, month, day from ct_date";
                pstat = connection.prepareStatement(queryDateSql);
                rs = pstat.executeQuery();
                while (rs.next()) {
                    Integer id = rs.getInt(1);
                    String year = rs.getString(2);
                    String month = rs.getString(3);
                    if (month.length() == 1) {
                        month = "0" + month;
                    }
                    String day = rs.getString(4);
                    if (day.length() == 1) {
                        day = "0" + day;
                    }
                    dateMap.put(year + month + day, id);
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
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
         * 输出数据
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(Text key, Text value) throws IOException, InterruptedException {

            String[] values = value.toString().split("_");
            int sumCall = Integer.parseInt(values[0]);
            int sumDuration = Integer.parseInt(values[1]);

            String[] ks = key.toString().split("_");

            String tel = ks[0];
            String date = ks[1];


            int tel_id = userMap.get(tel);
            int date_id = dateMap.get(date);

            PreparedStatement pstat = null;
            try {
                String insertSQL = "insert into calllog ( tel_id, date_id, sumcall, sumduration ) values ( ?, ?, ?, ? )";
                pstat = connection.prepareStatement(insertSQL);


                pstat.setInt(1, tel_id);
                pstat.setInt(2, date_id);
                pstat.setInt(3, sumCall);
                pstat.setInt(4, sumDuration);
                pstat.executeUpdate();
            } catch (SQLException e) {
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
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}


```



##### 3.10安装Redis

###### 解压

 sudo tar -zxvf redis-5.0.6.tar.gz -C /opt/module/

###### 安装gcc

sudo yum install gcc	

###### 安装Redis

执行 make  

进入root用户 执行	make install

查看安装目录	cd /usr/local/bin/

###### 配置

在根目录创建文件夹	mkdir myredis

将redis.conf移动到myredis	cp redis.conf /myredis/

在myredis目录中编辑文件	vim redis.conf	

设置	daemonize 为 yes

设置 	logfile 目录(在redis-server同级目录下新建redis_log.log)

###### 配置环境变量

#Redis
export REDIS_HOME=/opt/module/redis-5.0.6
export PATH=$PATH:$REDIS_HOME/src

###### 启动

redis-server /myredis/redis.conf

###### 查看客户端

redis-cli -p 6379

##### 3.11使用Redis保存缓存数据

###### 3.11.1设置redis缓存

```java
/**
 * @Time : 2022/12/5 14:00
 * @Author : jin
 * @File : Bootstrap.class
 */
package org.fengyue.ct_web.cache;

import JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 启动缓存客户端，向redis中增加缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {


        //读取mysql数据
        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();

        //读取用户和时间数据
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = JDBCUtil.getConnection();

            String queryUserSql = "select id,tel from contacts";
            preparedStatement = connection.prepareStatement(queryUserSql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Integer id = resultSet.getInt(1);
                String tel = resultSet.getString(2);
                userMap.put(tel, id);
            }
            resultSet.close();
            //时间数据
            String queryDateSql = "select * from calldate";
            preparedStatement = connection.prepareStatement(queryDateSql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Integer id = resultSet.getInt(1);
                String year = resultSet.getString(2);
                String month = resultSet.getString(3);
                //判断月份补0
                if (month.length() == 1) {
                    month = "0" + month;
                }
                String day = resultSet.getString(4);
                if (day.length() == 1) {
                    day = "0" + day;
                }
                dateMap.put(year + month + day, id);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
//        System.out.println(userMap.size());
//        System.out.println(dateMap.size());

        //向redis存储
        Jedis jedis = new Jedis("hadoop101", 6379);

        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("contacts", key, "" + value);
        }

        keyIterator = dateMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = dateMap.get(key);
            jedis.hset("calldate", key, "" + value);
        }

    }
}
```

###### 3.11.2修改OPF

```java
/**
 * @Time : 2022/12/4 19:19
 * @Author : jin
 * @File : MySQLRedisTextOutPutFormat.class
 */
package org.fengyue.ct_web.analysis.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;


/**
 * MySQL数据格式化输出对象
 */
public class MySQLRedisTextOutPutFormat extends OutputFormat<Text, Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {

        private Connection connection = null;
        private Jedis jedis = null;

        public MySQLRecordWriter() {
            // 获取资源
            connection = JDBCUtil.getConnection();
            jedis = new Jedis("hadoop101", 6379);
        }

        /**
         * 输出数据
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(Text key, Text value) throws IOException, InterruptedException {

            String[] values = value.toString().split("_");
            int sumCall = Integer.parseInt(values[0]);
            int sumDuration = Integer.parseInt(values[1]);

            String[] ks = key.toString().split("_");

            String tel = jedis.hget("contacts", ks[0]);
            String date = jedis.hget("calldate", ks[1]);

            int tel_id = -1, date_id = -1;
            if (null != tel) tel_id = Integer.parseInt(tel);
            if (null != date) date_id = Integer.parseInt(date);

            PreparedStatement pstat = null;
            try {
                if (tel_id != -1 && date_id != -1) {
                    String insertSQL = "insert into calllog ( tel_id, date_id, sumcall, sumduration ) values ( ?, ?, ?, ? )";
                    pstat = connection.prepareStatement(insertSQL);


                    pstat.setInt(1, tel_id);
                    pstat.setInt(2, date_id);
                    pstat.setInt(3, sumCall);
                    pstat.setInt(4, sumDuration);
                    pstat.executeUpdate();
                }
            } catch (SQLException e) {
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
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}

```

###### 3.11.3打包测试

**注意：需要先将redis jar包上传到hadoop依赖文件夹中**

##### ![image-20221205155534695](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221205155534695.png)3.12自定义K V

###### K V

```java
/**
 * @Time : 2022/12/5 15:57
 * @Author : jin
 * @File : AnalysisKey.class
 */
package org.fengyue.ct_web.analysis.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据Key
 */
public class AnalysisKey implements WritableComparable<AnalysisKey> {
    private String tel;
    private String date;

    public AnalysisKey() {
    }

    public AnalysisKey(String tel, String date) {
        this.date = date;
        this.tel = tel;
    }

    /**
     * 比较 ：tel date
     * @param key
     * @return
     */
    @Override
    public int compareTo(AnalysisKey key) {
        int result = tel.compareTo(key.getTel());
        if (result == 0) {
            result = date.compareTo(key.getDate());
        }
        return result;
    }

    /**
     * 读数据
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tel);
        out.writeUTF(date);
    }

    /**
     * 写数据
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        tel = in.readUTF();
        date = in.readUTF();
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
```

```java
/**
 * @Time : 2022/12/5 15:58
 * @Author : jin
 * @File : AnalysisValue.class
 */
package org.fengyue.ct_web.analysis.kv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据Value
 */
public class AnalysisValue implements Writable {
    private String sumCall;
    private String sumDuration;

    public AnalysisValue() {
    }

    public AnalysisValue(String sumCall, String sumDuration) {
        this.sumCall = sumCall;
        this.sumDuration = sumDuration;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sumCall);
        out.writeUTF(sumDuration);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sumCall = in.readUTF();
        sumDuration = in.readUTF();
    }

    public String getSumCall() {
        return sumCall;
    }

    public void setSumCall(String sumCall) {
        this.sumCall = sumCall;
    }

    public String getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(String sumDuration) {
        this.sumDuration = sumDuration;
    }
}
```

###### Mapper

```
/**
 * @Time : 2022/12/4 19:11
 * @Author : jin
 * @File : AnalysisTextMapper.class
 */
package org.fengyue.ct_web.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import AnalysisKey;

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
```

###### Reducer

```java
/**
 * @Time : 2022/12/4 19:15
 * @Author : jin
 * @File : AnalysisTextReduce.class
 */
package org.fengyue.ct_web.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import AnalysisKey;
import AnalysisValue;

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
        context.write(key, new AnalysisValue("" + sumCall, "" + sumDuration));
    }

}
```

###### OutPutFormat

```java
/**
 * @Time : 2022/12/4 19:19
 * @Author : jin
 * @File : MySQLRedisBeanOutPutFormat.class
 */
package org.fengyue.ct_web.analysis.io;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import AnalysisKey;
import AnalysisValue;
import JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;


/**
 * MySQL数据格式化输出对象
 */
public class MySQLRedisBeanOutPutFormat extends OutputFormat<AnalysisKey, AnalysisValue> {
    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue> {
        private Connection connection = null;
        private Jedis jedis = null;

        public MySQLRecordWriter() {
            //获取资源
            connection = JDBCUtil.getConnection();
            jedis = new Jedis("hadoop101", 6379);

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
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {


            int sumCall = Integer.parseInt(value.getSumCall());
            int sumDuration = Integer.parseInt(value.getSumDuration());


            String tel = jedis.hget("contacts", key.getTel());
            String date = jedis.hget("calldate", key.getDate());

            int tel_id = -1, date_id = -1;
            if (null != tel) tel_id = Integer.parseInt(tel);
            if (null != date) date_id = Integer.parseInt(date);


            PreparedStatement pstat = null;
            try {
                if (tel_id != -1 && date_id != -1) {

                    String insertSQL = "insert into calllog (tel_id,date_id,sumcall,sumduration) values (?, ?, ? ,?)";
                    pstat = connection.prepareStatement(insertSQL);


                    pstat.setInt(1, tel_id);
                    pstat.setInt(2, date_id);
                    pstat.setInt(3, sumCall);
                    pstat.setInt(4, sumDuration);

                    pstat.executeUpdate();
                }

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
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (jedis != null) {
                try {
                    jedis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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
```

###### Toll

```java
/**
 * @Time : 2022/12/4 18:44
 * @Author : jin
 * @File : AnalysisTextTool.class
 */
package org.fengyue.ct_web.analysis.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import MySQLRedisBeanOutPutFormat;
import AnalysisKey;
import AnalysisValue;
import AnalysisBeanMapper;
import AnalysisBeanReducer;
import Names;


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
```

#### 四.数据展示

##### 4.1创建SpringBoot项目

##### 4.2Entity

```java
/**
 * @Time : 2022/12/6 16:21
 * @Author : jin
 * @File : Calllog.class
 */
package org.fengyue.hadoopctweb.entity;

public class Calllog {
    private Integer id;
    private Integer tel_id;
    private Integer date_id;
    private Integer sumcall;
    private Integer sumduration;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getTel_id() {
        return tel_id;
    }

    public void setTel_id(Integer tel_id) {
        this.tel_id = tel_id;
    }

    public Integer getDate_id() {
        return date_id;
    }

    public void setDate_id(Integer date_id) {
        this.date_id = date_id;
    }

    public Integer getSumcall() {
        return sumcall;
    }

    public void setSumcall(Integer sumcall) {
        this.sumcall = sumcall;
    }

    public Integer getSumduration() {
        return sumduration;
    }

    public void setSumduration(Integer sumduration) {
        this.sumduration = sumduration;
    }


    @Override
    public String toString() {
        return "Calllog{" +
                "id=" + id +
                ", tel_id=" + tel_id +
                ", date_id=" + date_id +
                ", sumcall=" + sumcall +
                ", sumduration=" + sumduration + '}';
    }
}
```

##### 4.3Dao

```java
package org.fengyue.hadoopctweb.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.fengyue.hadoopctweb.entity.Calllog;

import java.util.List;
import java.util.Map;

@Mapper
public interface CalllogDao {

    @Select("select * from calllog where tel_id= (SELECT id from ct_user where tel=#{tel}) and date_id in (SELECT ID from ct_date where year=#{year} and month !='-1'  and  day!='-1')")
    List<Calllog> queryMonthDatas(Map<String, Object> paramMap);
}
```

4.4Service

```java
package org.fengyue.hadoopctweb.service;

import org.fengyue.hadoopctweb.entity.Calllog;

import java.util.List;

public interface CalllogService {
    List<Calllog> queryMonthDatas(String tel, String calltime);

}

```

##### 4.5ServiceImpl

```java
/**
 * @Time : 2022/12/6 15:57
 * @Author : jin
 * @File : CalllogServiceImpl.class
 */
package org.fengyue.hadoopctweb.service.impl;

import org.fengyue.hadoopctweb.dao.CalllogDao;
import org.fengyue.hadoopctweb.entity.Calllog;
import org.fengyue.hadoopctweb.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class CalllogServiceImpl implements CalllogService {
    @Autowired
    CalllogDao calllogDao;

    /**
     * 查询用户指定时间的通话统计信息
     *
     * @param tel
     * @param calltime
     * @return
     */
    @Override
    public List<Calllog> queryMonthDatas(String tel, String calltime) {


        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("tel", tel);

        if (calltime.length() > 4) {
            calltime = calltime.substring(0, 4);
        }
        paramMap.put("year", calltime);
        System.out.println("**************************************");
        System.out.println(paramMap);
        System.out.println("**************************************");


        return calllogDao.queryMonthDatas(paramMap);
    }


}
```

##### 4.6Conteoller

```java
/**
 * @Time : 2022/12/6 15:58
 * @Author : jin
 * @File : CalllogController.class
 */
package org.fengyue.hadoopctweb.controller;

import org.fengyue.hadoopctweb.entity.Calllog;
import org.fengyue.hadoopctweb.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import java.util.List;

@Controller
public class CalllogController {
    @Autowired
    private CalllogService calllogService;

    @RequestMapping("/query_clog")
    public String query() {
        return "query.jsp";
    }

    // Object ==> json ==> String
    //@ResponseBody
    @RequestMapping("/view")
    public String view(String tel, String calltime, Model model) {

        List<Calllog> logs = calllogService.queryMonthDatas(tel, calltime);

        System.out.println("**************************************");
        System.out.println(logs);
        System.out.println("**************************************");

        model.addAttribute("clogs", logs);

        return "view.jsp";
    }

}
```

##### 4.7查询界面

```html
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8" %>
<%@ page import="org.apache.hadoop.fs.FileStatus" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%
    FileStatus[] filelist = (FileStatus[]) request.getAttribute("filelist");
    String currenturl = "";
    String currenturl1 = "";
    if (request.getAttribute("currenturl") != null) {
        currenturl = request.getAttribute("currenturl").toString();
        currenturl1 = request.getAttribute("currenturl1").toString();
    }

%>

<html>
<head>
    <meta charset="UTF-8"/>
    <title>网盘首页</title>
    <link rel="stylesheet" type="text/css" href="css/common.css"/>
    <link rel="stylesheet" type="text/css" href="css/main.css"/>
</head>
<body>

<!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
<script src="./js/jquery-2.1.1.min.js"></script>
<!-- Include all compiled plugins (below), or include individual files as needed -->
<script src="./bootstrap/js/bootstrap.js"></script>

<script>

    // 声明方法
    function queryData() {
        window.location.href = "/view?tel=" + $("#tel").val() + "&calltime=" + $("#calltime").val();
    }
</script>

<div class="container clearfix">
    <div class="sidebar-wrap">
        <div class="sidebar-title">
            <h1>网盘首页</h1>
        </div>
        <div class="sidebar-content">
            <ul class="sidebar-list">
                <li>
                    <a href="#"><i class="icon-font">&#xe003;</i>常用操作</a>
                    <ul class="sub-menu">
                        <li><a href="readdir?url=/"><i class="icon-font">&#xe008;</i>个人文件</a></li>
                        <li><a href="query_clog"><i class="icon-font">&#xe005;</i>群组文件</a></li>
                        <li><a href="#"><i class="icon-font">&#xe006;</i>我的收藏</a></li>
                        <li><a href="#"><i class="icon-font">&#xe004;</i>本地同步</a></li>
                        <li><a href="#"><i class="icon-font">&#xe012;</i>我的分享</a></li>
                        <li><a href="#"><i class="icon-font">&#xe052;</i>我的订阅</a></li>
                        <li><a href="#"><i class="icon-font">&#xe033;</i>标签分类</a></li>
                        <li><a href="#"><i class="icon-font">&#xe033;</i>回收站</a></li>
                    </ul>
                </li>
                <li>
                    <a href="#"><i class="icon-font">&#xe018;</i>帮助中心</a>
                    <ul class="sub-menu">
                        <li><a href="#"><i class="icon-font">&#xe017;</i>系统设置</a></li>
                        <li><a href="#"><i class="icon-font">&#xe037;</i>清理缓存</a></li>

                    </ul>
                </li>
            </ul>
        </div>
    </div>


    <!--/sidebar-->
    <div class="main-wrap">
        <div class="container" style="float: right ; padding-right: 10px;margin-top: 7px">
            <form action="query">
                <input type="text" name="searchName">
                <input type="submit" value="搜索一下">
            </form>
        </div>
        <div class="crumb-wrap">
            <div class="crumb-list"><i class="icon-font">&#xe06b;</i><span>当前路径>><a
                    href="readdir?url=/">个人文件</a> </span>

                <%
                    // hdfs:///rt/11
                    String[] splits = currenturl.split("/");
                    //判断
                    if (splits.length > 3) {
                        String path = splits[0] + "/" + splits[1] + "/" + splits[2] + "/" + splits[3];
                        int len = 0;
                        //for循环
                        for (int i = 4; i < splits.length; i++) {
                            len = path.length();
                            path = path + "/" + splits[i];
                            String path1 = path.substring(len, path.length());
                %>
                <a href="readdir?url=<%=path%>">&nbsp;&nbsp;&nbsp;<%=path1 %>
                </a>
                <%}%>
                <% } else {%>
                <a href="readdir?url=<%=currenturl%>"><%=currenturl1 %>
                </a>
                <% }%>
            </div>
        </div>
        <div class="result-wrap">
            <div class="result-title">
                <h1>快捷操作</h1>
            </div>
            <div class="result-content">
                <div class="short-wrap">
                    <form method="post" action="/upload?url=<%=currenturl %>" enctype="multipart/form-data" id="myfrm">
                        <a href="javascript:;" class="a-upload"><input type="file" name="file" id="file"
                                                                       onchange="uploadfile()"><i class="icon-font">&#xe005;</i>上传文件</a>
                        <script language="JavaScript">
                            function uploadfile() {
                                document.getElementById("myfrm").submit();
                            }
                        </script>
                        <a href="javascript:createdir()" class="a-upload"><input type="button" name="" id="createdir"><i
                                class="icon-font">&#xe005;</i>新建文件夹</a>
                        <script language="JavaScript">
                            function createdir() {
                                var dirname = prompt('请输入文件夹名称', '新建文件夹1');
                                if (dirname.length > 0) {
                                    location.href = "createdir?newdirname=<%=currenturl1 %>/" + dirname;
                                }
                            }
                        </script>
                    </form>
                </div>
            </div>
        </div>
        <div class="result-wrap">
            <div class="result-title">
                <h1>通话时间查询</h1>
            </div>
            <div class="result-content">
                <div>
                    <form>
                        <div class="form-group">
                            <label for="tel">电话号码：</label>
                            <input type="text" class="form-control" id="tel" placeholder="请输入电话号码">
                        </div>
                        <div class="form-group">
                            <label for="calltime">查询时间：</label>
                            <input type="date" class="form-control" id="calltime"/>
                        </div>
                        <button type="button" class="btn btn-default" onclick="queryData()">查询</button>
                    </form>
                </div>

            </div>
        </div>
    </div>
</div>
</body>
</html>
```

##### 4.8Echarts图例

```html
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8" %>
<%@ page import="org.apache.hadoop.fs.FileStatus" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%
    FileStatus[] filelist = (FileStatus[]) request.getAttribute("filelist");
    String currenturl = "";
    String currenturl1 = "";
    if (request.getAttribute("currenturl") != null) {
        currenturl = request.getAttribute("currenturl").toString();
        currenturl1 = request.getAttribute("currenturl1").toString();
    }

%>

<html>
<head>
    <meta charset="UTF-8"/>
    <title>网盘首页</title>
    <link rel="stylesheet" type="text/css" href="css/common.css"/>
    <link rel="stylesheet" type="text/css" href="css/main.css"/>
    <script src="./js/jquery-2.1.1.min.js"></script>
    <script src="./bootstrap/js/bootstrap.js"></script>
    <script src="./js/echarts.min.js"></script>
</head>
<body>
<div class="container clearfix">
    <div class="sidebar-wrap">
        <div class="sidebar-title">
            <h1>网盘首页</h1>
        </div>
        <div class="sidebar-content">
            <ul class="sidebar-list">
                <li>
                    <a href="#"><i class="icon-font">&#xe003;</i>常用操作</a>
                    <ul class="sub-menu">
                        <li><a href="readdir?url=/"><i class="icon-font">&#xe008;</i>个人文件</a></li>
                        <li><a href="query_clog"><i class="icon-font">&#xe005;</i>群组文件</a></li>
                        <li><a href="#"><i class="icon-font">&#xe006;</i>我的收藏</a></li>
                        <li><a href="#"><i class="icon-font">&#xe004;</i>本地同步</a></li>
                        <li><a href="#"><i class="icon-font">&#xe012;</i>我的分享</a></li>
                        <li><a href="#"><i class="icon-font">&#xe052;</i>我的订阅</a></li>
                        <li><a href="#"><i class="icon-font">&#xe033;</i>标签分类</a></li>
                        <li><a href="#"><i class="icon-font">&#xe033;</i>回收站</a></li>
                    </ul>
                </li>
                <li>
                    <a href="#"><i class="icon-font">&#xe018;</i>帮助中心</a>
                    <ul class="sub-menu">
                        <li><a href="#"><i class="icon-font">&#xe017;</i>系统设置</a></li>
                        <li><a href="#"><i class="icon-font">&#xe037;</i>清理缓存</a></li>

                    </ul>
                </li>
            </ul>
        </div>
    </div>


    <!--/sidebar-->
    <div class="main-wrap">
        <div class="container" style="float: right ; padding-right: 10px;margin-top: 7px">
            <form action="query">
                <input type="text" name="searchName">
                <input type="submit" value="搜索一下">
            </form>
        </div>
        <div class="crumb-wrap">
            <div class="crumb-list"><i class="icon-font">&#xe06b;</i><span>当前路径>><a
                    href="readdir?url=/">个人文件</a> </span>

                <%
                    // hdfs:///rt/11
                    String[] splits = currenturl.split("/");
                    //判断
                    if (splits.length > 3) {
                        String path = splits[0] + "/" + splits[1] + "/" + splits[2] + "/" + splits[3];
                        int len = 0;
                        //for循环
                        for (int i = 4; i < splits.length; i++) {
                            len = path.length();
                            path = path + "/" + splits[i];
                            String path1 = path.substring(len, path.length());
                %>
                <a href="readdir?url=<%=path%>">&nbsp;&nbsp;&nbsp;<%=path1 %>
                </a>
                <%}%>
                <% } else {%>
                <a href="readdir?url=<%=currenturl%>"><%=currenturl1 %>
                </a>
                <% }%>
            </div>
        </div>
        <div class="result-wrap">
            <div class="result-title">
                <h1>快捷操作</h1>
            </div>
            <div class="result-content">
                <div class="short-wrap">
                    <form method="post" action="/upload?url=<%=currenturl %>" enctype="multipart/form-data" id="myfrm">
                        <a href="javascript:;" class="a-upload"><input type="file" name="file" id="file"
                                                                       onchange="uploadfile()"><i class="icon-font">&#xe005;</i>上传文件</a>
                        <script language="JavaScript">
                            function uploadfile() {
                                document.getElementById("myfrm").submit();
                            }
                        </script>
                        <a href="javascript:createdir()" class="a-upload"><input type="button" name="" id="createdir"><i
                                class="icon-font">&#xe005;</i>新建文件夹</a>
                        <script language="JavaScript">
                            function createdir() {
                                var dirname = prompt('请输入文件夹名称', '新建文件夹1');
                                if (dirname.length > 0) {
                                    location.href = "createdir?newdirname=<%=currenturl1 %>/" + dirname;
                                }
                            }
                        </script>
                    </form>
                </div>
            </div>
        </div>
        <div class="result-wrap">
            <div class="result-title">
                <h1>文件基本信息</h1>
            </div>
            <div class="result-content">
                <div id="main" style="width: 600px ;height: 400px"></div>
            </div>
        </div>
    </div>

    <script>
        // 基于准备好的dom，初始化echarts实例
        const myChart = echarts.init(document.getElementById('main'));

        option = {
            title: {
                text: '用户通话统计',
                subtext: '纯属虚构'
            },
            tooltip: {
                trigger: 'axis'
            },
            legend: {
                data: ['通话次数', '通话时长']
            },
            toolbox: {
                show: true,
                feature: {
                    dataView: {show: true, readOnly: false},
                    magicType: {show: true, type: ['line', 'bar']},
                    restore: {show: true},
                    saveAsImage: {show: true}
                }
            },
            calculable: true,
            xAxis: [
                {
                    type: 'category',
                    data: [
                        <c:forEach items="${clogs}" var="calllog" >
                        ${calllog.date_id},
                        </c:forEach>
                    ]
                }
            ],
            yAxis: [
                {
                    type: 'value'
                }
            ],
            series: [
                {
                    name: '通话次数',
                    type: 'bar',
                    data: [
                        <c:forEach items="${clogs}" var="calllog" >
                        ${calllog.sumcall},
                        </c:forEach>
                    ],
                    markPoint: {
                        data: [
                            {type: 'max', name: '最大值'},
                            {type: 'min', name: '最小值'}
                        ]
                    },
                    markLine: {
                        data: [
                            {type: 'average', name: '平均值'}
                        ]
                    }
                },
                {
                    name: '通话时长',
                    type: 'bar',
                    data: [
                        <c:forEach items="${clogs}" var="calllog" >
                        ${calllog.sumduration},
                        </c:forEach>
                    ],
                    markPoint: {
                        data: [
                            {name: '年最高', value: 182.2, xAxis: 7, yAxis: 183},
                            {name: '年最低', value: 2.3, xAxis: 11, yAxis: 3}
                        ]
                    },
                    markLine: {
                        data: [
                            {type: 'average', name: '平均值'}
                        ]
                    }
                }
            ]
        };

        // 使用刚指定的配置项和数据显示图表。
        myChart.setOption(option);
    </script>

</div>
</body>
</html>
```



##### 4.9展示

![image-20221206204915458](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221206204915458.png)

![image-20221206204846777](C:\Users\从你的全世界路过\AppData\Roaming\Typora\typora-user-images\image-20221206204846777.png)
