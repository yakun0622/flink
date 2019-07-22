package com.acronsh.task;

import com.acronsh.entity.CarrierInfo;
import com.acronsh.entity.EmailInfo;
import com.acronsh.map.CarrierMap;
import com.acronsh.map.EmailMap;
import com.acronsh.reduce.CarrierReduce;
import com.acronsh.reduce.EmailTypeReduce;
import com.acronsh.util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 16:25
 */
public class EmailTypeTask {
    public static void main(String[] args) {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<EmailInfo> mapResult = text.map(new EmailMap());
        DataSet<EmailInfo> reduceResult = mapResult.groupBy("groupFiled").reduce(new EmailTypeReduce());
        try {
            List<EmailInfo> list = reduceResult.collect();
            for (EmailInfo emailInfo : list) {
                Document document = MongoUtils.findOne("email_statistics", "portrait", emailInfo.getEmailType());
                if (document == null) {
                    document = new Document();
                    document.put("info", emailInfo.getEmailType());
                    document.put("count", emailInfo.getCount());
                } else {
                    document.put("count", document.getLong("count") + emailInfo.getCount());
                }
                MongoUtils.saveOrUpdate("email_statistics", "portrait", document);

            }
            env.execute("email analyse");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
