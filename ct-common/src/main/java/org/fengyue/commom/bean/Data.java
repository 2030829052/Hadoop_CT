/**
 * @Time : 2022/12/3 11:59
 * @Author : jin
 * @File : Data.class
 */
package org.fengyue.commom.bean;

/**
 * 数据对象
 */
public abstract class Data implements Value {

    public String content;

    @Override
    public void setVal(Object val) {
        content = (String) val;
    }

    @Override
    public Object getVal() {
        return content;
    }
}

