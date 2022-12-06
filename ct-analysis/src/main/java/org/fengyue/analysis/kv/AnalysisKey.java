/**
 * @Time : 2022/12/5 15:57
 * @Author : jin
 * @File : AnalysisKey.class
 */
package org.fengyue.analysis.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据Key
 */
public class AnalysisKey implements WritableComparable<AnalysisKey> {
    private String tel;
    private String date;

    public AnalysisKey() {
    }

    public AnalysisKey(String tel, String date) {
        this.date = date;
        this.tel = tel;
    }

    /**
     * 比较 ：tel date
     * @param key
     * @return
     */
    @Override
    public int compareTo(AnalysisKey key) {
        int result = tel.compareTo(key.getTel());
        if (result == 0) {
            result = date.compareTo(key.getDate());
        }
        return result;
    }

    /**
     * 读数据
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tel);
        out.writeUTF(date);
    }

    /**
     * 写数据
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        tel = in.readUTF();
        date = in.readUTF();
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}