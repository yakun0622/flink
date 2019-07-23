/*
 * Copyright (c) 2017 Hinew. All Rights Reserved.
 * ============================================================================
 * 版权所有 海牛(上海)电子商务有限公司，并保留所有权利。
 * ----------------------------------------------------------------------------
 * ----------------------------------------------------------------------------
 * 官方网站：http://www.hinew.com.cn
 * ============================================================================
 */
package com.acronsh.map;

import com.acronsh.entity.BaiJiaInfo;
import com.acronsh.entity.CarrierInfo;
import com.acronsh.util.CarrierUtils;
import com.acronsh.util.HbaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 17:13
 */
public class BaiJiaMap implements MapFunction<String, BaiJiaInfo> {

    @Override
    public BaiJiaInfo map(String s) throws Exception {
        if (StringUtils.isEmpty(s)){
            return null;
        }
        String[] orderInfo = s.split(",");
        BaiJiaInfo baiJiaInfo = new BaiJiaInfo();
        baiJiaInfo.setUserId(orderInfo[12]);
        baiJiaInfo.setCreateTime(orderInfo[1]);
        baiJiaInfo.setAmount(orderInfo[4]);
        baiJiaInfo.setPayType(orderInfo[5]);
        baiJiaInfo.setPayTime(orderInfo[6]);
        baiJiaInfo.setPayStatus(orderInfo[7]);
        baiJiaInfo.setCouponAmount(orderInfo[8]);
        baiJiaInfo.setTotalAmount(orderInfo[9]);
        baiJiaInfo.setRefundAmount(orderInfo[10]);
        baiJiaInfo.setNum(orderInfo[11]);
        List<BaiJiaInfo> list = new ArrayList<>();
        list.add(baiJiaInfo);
        return baiJiaInfo;
    }
}
