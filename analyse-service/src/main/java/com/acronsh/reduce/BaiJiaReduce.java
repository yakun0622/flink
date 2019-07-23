package com.acronsh.reduce;

import com.acronsh.entity.BaiJiaInfo;
import com.acronsh.entity.CarrierInfo;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/23 00:06
 */
public class BaiJiaReduce implements ReduceFunction<BaiJiaInfo> {

    @Override
    public BaiJiaInfo reduce(BaiJiaInfo baiJiaInfo, BaiJiaInfo t1) throws Exception {
        List<BaiJiaInfo> finalList = new ArrayList<>();
        finalList.addAll(baiJiaInfo.getList());
        finalList.addAll(t1.getList());

        /**
         * 将同一个用户的购买数据进行汇总
         */
        BaiJiaInfo finalBaiJiaInfo = new BaiJiaInfo();
        finalBaiJiaInfo.setUserId(baiJiaInfo.getUserId());
        finalBaiJiaInfo.setList(finalList);
        return finalBaiJiaInfo;
    }
}
