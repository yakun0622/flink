package com.acronsh.map;

import com.acronsh.entity.YearBase;
import com.acronsh.util.DateUtils;
import com.acronsh.util.HbaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 17:13
 */
public class YearBaseMap implements MapFunction<String, YearBase> {

    @Override
    public YearBase map(String s) throws Exception {
        if (StringUtils.isEmpty(s)){
            return null;
        }
        String[] userInfo = s.split(",");
        String userId = userInfo[0];
        String username = userInfo[1];
        String sex = userInfo[2];
        String telephone = userInfo[3];
        String email = userInfo[4];
        String age = userInfo[5];
        String registerTime = userInfo[6];
        // 终端类型：0-PC端 1-移动端  2-小程序端
        String userType = userInfo[7];
        String yearBaseStr = DateUtils.getYearBaseByAge(age);

        String tableName = "user_flag_info";
        String rowKey = userId;
        String familyName = "baseInfo";
        String column = "yearBase";
        HbaseUtil.putdata(tableName, rowKey, familyName, column, yearBaseStr);

        YearBase yearBase = new YearBase();
        yearBase.setCount(1L);
        yearBase.setYearType(yearBaseStr);
        yearBase.setGroupFiled("yearBase==" + yearBaseStr);

        return yearBase;
    }
}
