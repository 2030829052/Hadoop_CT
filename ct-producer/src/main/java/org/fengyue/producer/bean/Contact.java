/**
 * @Time : 2022/12/3 13:15
 * @Author : jin
 * @File : Contact.class
 */
package org.fengyue.producer.bean;

import org.fengyue.commom.bean.Data;

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
