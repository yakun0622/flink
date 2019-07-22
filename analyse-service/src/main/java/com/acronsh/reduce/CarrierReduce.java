/*
 * Copyright (c) 2017 Hinew. All Rights Reserved.
 * ============================================================================
 * 版权所有 海牛(上海)电子商务有限公司，并保留所有权利。
 * ----------------------------------------------------------------------------
 * ----------------------------------------------------------------------------
 * 官方网站：http://www.hinew.com.cn
 * ============================================================================
 */
package com.acronsh.reduce;

import com.acronsh.entity.CarrierInfo;
import com.acronsh.entity.YearBase;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/23 00:06
 */
public class CarrierReduce implements ReduceFunction<CarrierInfo> {
    @Override
    public CarrierInfo reduce(CarrierInfo carrierInfo, CarrierInfo t1) throws Exception {
        CarrierInfo finalCarrier = new CarrierInfo();
        finalCarrier.setCarrierType(carrierInfo.getCarrierType());
        finalCarrier.setCount(carrierInfo.getCount() + t1.getCount());
        return finalCarrier;
    }
}
