package com.acronsh.task;

import cn.hutool.core.date.DateUtil;
import com.acronsh.entity.BaiJiaInfo;
import com.acronsh.entity.CarrierInfo;
import com.acronsh.map.BaiJiaMap;
import com.acronsh.map.CarrierMap;
import com.acronsh.reduce.BaiJiaReduce;
import com.acronsh.reduce.CarrierReduce;
import com.acronsh.util.DateUtils;
import com.acronsh.util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 16:25
 */
public class BaiJiaTask {
    public static void main(String[] args) {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<BaiJiaInfo> mapResult = text.map(new BaiJiaMap());
        DataSet<BaiJiaInfo> reduceResult = mapResult.groupBy("groupFiled").reduce(new BaiJiaReduce());

        try {
            // 处理汇总后的数据
            List<BaiJiaInfo> resultList = reduceResult.collect();

            // 按照时间排序
            baiJiaDataHandle(resultList);


            env.execute("carrier analyse");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<BaiJiaInfo> baiJiaDataHandle(List<BaiJiaInfo> resultList) {
        for (BaiJiaInfo baijiaInfo : resultList) {
            String userId = baijiaInfo.getUserId();
            List<BaiJiaInfo> list = baijiaInfo.getList();
            // 先把每个用户的消费信息 按照时间排序
            Collections.sort(list, (o1, o2) -> {
                String time1 = o1.getCreateTime();
                String time2 = o2.getCreateTime();
                DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmss");

                Date date1 = new Date();
                Date date2 = date1;
                try {
                    date2 = dateFormat.parse(time2);
                    date1 = dateFormat.parse(time1);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return date1.compareTo(date2);
            });

            BaiJiaInfo before = null;
            Map<Integer, Integer> frequencyMap = new HashMap<>();
            // 用户购买最大金额
            double maxAmount = 0.0d;
            // 用户消费金额汇总
            double sum = 0d;

            for (BaiJiaInfo baiJiaInfoItem : list) {
                if (before == null) {
                    before = baiJiaInfoItem;
                    maxAmount = Double.valueOf(before.getTotalAmount());
                    sum += maxAmount;
                    continue;
                }
                // 计算购买的频率
                int days = 0;
                try {
                    days = DateUtils.getDaysBetweenbyStartAndend(before.getCreateTime(), baiJiaInfoItem.getCreateTime(), "yyyyMMdd hhmmss");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                int beforeNum = frequencyMap.get(days) == null ? 0 : frequencyMap.get(days);
                frequencyMap.put(days, beforeNum + 1);

                //计算最大金额
                Double totalAmount = Double.valueOf(baiJiaInfoItem.getTotalAmount());
                if (totalAmount > maxAmount) {
                    maxAmount = totalAmount;
                }
                sum += totalAmount;
                before = baiJiaInfoItem;
            }

            //平均每次的消费金额
            double avramount = sum / list.size();

            //总天数
            int totaldays = 0;
            for (Map.Entry<Integer, Integer> entry : frequencyMap.entrySet()) {
                Integer frequencydays = entry.getKey();
                Integer count = entry.getValue();
                totaldays += frequencydays * count;

            }

            //平均多少天消费一次
            int avrdays = totaldays / list.size();

            // 败家指数 = 支付金额平均值*0.3 + 最大支付金额*0.3 + 下单频率*0.4
            // 支付金额平均值30分（0-20 5 20-60 10 60-100 20 100-150 30 150-200 40 200-250 60 250-350 70 350-450 80 450-600 90 600以上 100  ）
            // 最大支付金额30分（0-20 5 20-60 10 60-200 30 200-500 60 500-700 80 700 100）
            // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
            int avrAmountScore = 0;
            if (avramount >= 0 && avramount < 20) {
                avrAmountScore = 5;
            } else if (avramount >= 20 && avramount < 60) {
                avrAmountScore = 10;
            } else if (avramount >= 60 && avramount < 100) {
                avrAmountScore = 20;
            } else if (avramount >= 100 && avramount < 150) {
                avrAmountScore = 30;
            } else if (avramount >= 150 && avramount < 200) {
                avrAmountScore = 40;
            } else if (avramount >= 200 && avramount < 250) {
                avrAmountScore = 60;
            } else if (avramount >= 250 && avramount < 350) {
                avrAmountScore = 70;
            } else if (avramount >= 350 && avramount < 450) {
                avrAmountScore = 80;
            } else if (avramount >= 450 && avramount < 600) {
                avrAmountScore = 90;
            } else if (avramount >= 600) {
                avrAmountScore = 100;
            }

            int maxAmountScore = 0;
            if (maxAmount >= 0 && maxAmount < 20) {
                maxAmountScore = 5;
            } else if (maxAmount >= 20 && maxAmount < 60) {
                maxAmountScore = 10;
            } else if (maxAmount >= 60 && maxAmount < 200) {
                maxAmountScore = 30;
            } else if (maxAmount >= 200 && maxAmount < 500) {
                maxAmountScore = 60;
            } else if (maxAmount >= 500 && maxAmount < 700) {
                maxAmountScore = 80;
            } else if (maxAmount >= 700) {
                maxAmountScore = 100;
            }

            // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
            int avrdaysscore = 0;
            if (avrdays >= 0 && avrdays < 5) {
                avrdaysscore = 100;
            } else if (avramount >= 5 && avramount < 10) {
                avrdaysscore = 90;
            } else if (avramount >= 10 && avramount < 30) {
                avrdaysscore = 70;
            } else if (avramount >= 30 && avramount < 60) {
                avrdaysscore = 60;
            } else if (avramount >= 60 && avramount < 80) {
                avrdaysscore = 40;
            } else if (avramount >= 80 && avramount < 100) {
                avrdaysscore = 20;
            } else if (avramount >= 100) {
                avrdaysscore = 10;
            }
            double totalscore = (avrAmountScore / 100) * 30 + (maxAmountScore / 100) * 30 + (avrdaysscore / 100) * 40;

            String tablename = "userflaginfo";
            String rowkey = userId;
            String famliyname = "baseinfo";
            String colum = "baijiasoce";


        }
        return resultList;
    }
}
