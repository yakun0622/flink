package com.acronsh.task;

import com.acronsh.entity.CarrierInfo;
import com.acronsh.entity.YearBase;
import com.acronsh.map.CarrierMap;
import com.acronsh.map.YearBaseMap;
import com.acronsh.reduce.CarrierReduce;
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
public class CarrierInfoTask {
    public static void main(String[] args) {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<CarrierInfo> mapResult = text.map(new CarrierMap());
        DataSet<CarrierInfo> reduceResult = mapResult.groupBy("groupFiled").reduce(new CarrierReduce());
        try {
            List<CarrierInfo> list = reduceResult.collect();
            for (CarrierInfo carrierInfo : list) {
                Document document = MongoUtils.findOne("carrier_statistics", "portrait", carrierInfo.getCarrierType());
                if (document == null) {
                    document = new Document();
                    document.put("info", carrierInfo.getCarrierType());
                    document.put("count", carrierInfo.getCount());
                } else {
                    document.put("count", document.getLong("count") + carrierInfo.getCount());
                }
                MongoUtils.saveOrUpdate("carrier_statistics", "portrait", document);

            }
            env.execute("carrier analyse");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
