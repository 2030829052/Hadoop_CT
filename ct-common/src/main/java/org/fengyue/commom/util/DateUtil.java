/**
 * @Time : 2022/12/3 15:32
 * @Author : jin
 * @File : DateUtil.class
 */
package org.fengyue.commom.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    /**
     * 日期对象格式化
     *
     * @param dateString
     * @param format
     * @return
     */
    public static Date parse(String dateString, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date;

    }

    public static String format(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

}
