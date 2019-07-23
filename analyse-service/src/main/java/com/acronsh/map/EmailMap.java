package com.acronsh.map;

import com.acronsh.entity.EmailInfo;
import com.acronsh.util.EmailUtils;
import com.acronsh.util.HbaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 17:13
 */
public class EmailMap implements MapFunction<String, EmailInfo> {

    @Override
    public EmailInfo map(String s) throws Exception {
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
        String emailType = EmailUtils.getEmailtypeBy(email);

        String tableName = "user_flag_info";
        String rowKey = userId;
        String familyName = "baseInfo";
        String column = "emailInfo";
        HbaseUtil.putdata(tableName, rowKey, familyName, column, emailType);

        EmailInfo emailInfo = new EmailInfo();
        emailInfo.setEmailType(emailType);
        emailInfo.setCount(1L);
        emailInfo.setGroupFiled("emailInfo==" + emailType);

        return emailInfo;
    }
}
