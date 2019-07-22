package com.acronsh.task;

import com.acronsh.entity.YearBase;
import com.acronsh.map.YearBaseMap;
import com.acronsh.reduce.YearBaseReduce;
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
public class YearBaseTask {
    public static void main(String[] args) {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<YearBase> mapResult = text.map(new YearBaseMap());
        DataSet<YearBase> reduceResult = mapResult.groupBy("groupFiled").reduce(new YearBaseReduce());
        try {
            List<YearBase> list = reduceResult.collect();
            for (YearBase yearBase : list) {
                Document document = MongoUtils.findOne("year_base_statistics", "portrait", yearBase.getYearType());
                if (document == null) {
                    document = new Document();
                    document.put("info", yearBase.getYearType());
                    document.put("count", yearBase.getCount());
                } else {
                    document.put("count", document.getLong("count") + yearBase.getCount());
                }
                MongoUtils.saveOrUpdate("year_base_statistics", "portrait", document);

            }
            env.execute("year base");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
