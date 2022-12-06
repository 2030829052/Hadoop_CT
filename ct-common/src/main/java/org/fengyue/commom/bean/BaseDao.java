/**
 * @Time : 2022/12/4 1:03
 * @Author : jin
 * @File : BaseDao.class
 */
package org.fengyue.commom.bean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.fengyue.commom.api.Column;
import org.fengyue.commom.api.RowKey;
import org.fengyue.commom.api.TableRef;
import org.fengyue.commom.constant.Names;
import org.fengyue.commom.constant.ValueConstant;
import org.fengyue.commom.util.DateUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
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

    protected void createTableXX(String name, String... families) throws Exception {

        createTableXX(name, null, null, families);
    }


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
     * 增加多条数据
     */
    protected void putData(String name, List<Put> puts) throws IOException {

        //获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        //增加数据
        table.put(puts);
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

    /**
     * 获取查询时的startrow,stoprow集合
     *
     * @return
     */
    protected List<String[]> getStartStopRowKeys(String tel, String start, String end) {
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
