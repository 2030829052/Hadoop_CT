package org.fengyue.commom.bean;

import java.io.Closeable;

/**
 * 数据出口
 */
public interface DataOut extends Closeable {
    public void setPath(String path);
    public void write(Object data) throws Exception;
    public void write(String data) throws Exception;
}
