/**
 * @Time : 2022/12/3 12:50
 * @Author : jin
 * @File : LocalFileDataIn.class
 */
package org.fengyue.producer.io;

import org.fengyue.commom.bean.DataIn;
import org.fengyue.commom.bean.Data;

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
