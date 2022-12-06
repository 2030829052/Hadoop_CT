/**
 * @Time : 2022/12/4 16:05
 * @Author : jin
 * @File : InsertCalleeCoprocessor.class
 */
package org.fengyue.consumer.coprocessor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.client.Put;
import org.fengyue.commom.bean.BaseDao;
import org.fengyue.commom.constant.Names;

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

