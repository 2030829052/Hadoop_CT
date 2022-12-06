/**
 * @Time : 2022/12/3 12:38
 * @Author : jin
 * @File : LocalFileProducer.class
 */
package org.fengyue.producer.bean;

import org.fengyue.commom.bean.DataIn;
import org.fengyue.commom.bean.Producer;
import org.fengyue.commom.util.DateUtil;
import org.fengyue.commom.bean.DataOut;
import org.fengyue.commom.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;
    private volatile boolean flg = true;//volatile增强内存可见性

    @Override
    public void setIn(DataIn in) {
        this.in = in;
    }

    @Override
    public void setOut(DataOut out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    @Override
    public void produce() {

        try {
            //读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);
            while (flg) {
                //从通讯录随机查找电话号码（主叫，被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true) {
                    call2Index = new Random().nextInt(contacts.size());
                    if (call1Index != call2Index) {
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //生成随机通话时间
                String startDate = "20220101000000";
                String endDate = "20221212000000";
                long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();
                //通话时间
                long callTime = startTime + (long) ((endTime - startTime) * Math.random());
                //通话时间字符串
                String callTimeString = DateUtil.format(new Date(callTime), "yyyyMMddHHmmss");
                //生成通话时长 长度不够4为以0补全
                String duration = NumberUtil.format(new Random().nextInt(3000), 4);
                //生成通话记录
                Calllog log = new Calllog(call1.getTel(), call2.getTel(), callTimeString, duration);
                System.out.println(log);
                //保存通话记录
                out.write(log);
                //设置休眠
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭生产者
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }

    }
}
