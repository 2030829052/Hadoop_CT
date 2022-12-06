package org.fengyue.commom.constant;

import org.fengyue.commom.bean.Value;

/**
 * 名称常量枚举
 */
public enum Names implements Value {
    NAMESPACE("ct"),
    TOPIC("ct"),
    TABLE("ct:calllog"),
    CF_CALLER("caller"),
    CF_CALLEE("callee"),
    CF_INFO("info");
    private String name;

    private Names(String name) {
        this.name = name;
    }

    @Override
    public void setVal(Object val) {

    }

    @Override
    public String getVal() {
        return name;
    }
}
