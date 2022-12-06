package org.fengyue.commom.bean;

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
