/*
 * Copyright (c) 2017 Hinew. All Rights Reserved.
 * ============================================================================
 * 版权所有 海牛(上海)电子商务有限公司，并保留所有权利。
 * ----------------------------------------------------------------------------
 * ----------------------------------------------------------------------------
 * 官方网站：http://www.hinew.com.cn
 * ============================================================================
 */
package com.acronsh.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 17:26
 */
public class DateUtils {
    /**
     * 根据年龄获取年代（几零后）
     * @param age
     * @return
     */
    public static String getYearBaseByAge(String age){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.YEAR, -Integer.valueOf(age));
        Date birthDate = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
        String birthYear = sdf.format(birthDate);
        System.out.println("出生年份：" + birthYear);
        Integer year = Integer.valueOf(birthYear);
        return year.toString().charAt(2) + "0后";
    }

    public static void main(String[] args) {
        System.out.println(getYearBaseByAge("50"));
    }
}
