/**
 * @Time : 2022/12/3 12:51
 * @Author : jin
 * @File : LocalFileDataOut.class
 */
package org.fengyue.producer.io;

import org.fengyue.commom.bean.DataOut;

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
