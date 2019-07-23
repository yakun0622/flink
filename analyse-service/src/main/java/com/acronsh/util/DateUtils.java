package com.acronsh.util;

import java.text.DateFormat;
import java.text.ParseException;
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

    public static int getDaysBetweenbyStartAndend(String startTime,String endTime,String format) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(format);
        Date start = dateFormat.parse(startTime);
        Date end = dateFormat.parse(endTime);
        Calendar startCalendar = Calendar.getInstance();
        Calendar endCalendar = Calendar.getInstance();
        startCalendar.setTime(start);
        endCalendar.setTime(end);
        int days = 0;
        while(startCalendar.before(endCalendar)){
            startCalendar.add(Calendar.DAY_OF_YEAR,1);
            days += 1;
        }
        return days;
    }


    public static void main(String[] args) {
        System.out.println(getYearBaseByAge("50"));
    }
}
