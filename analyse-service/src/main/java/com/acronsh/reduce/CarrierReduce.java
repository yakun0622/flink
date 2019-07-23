package com.acronsh.reduce;

import com.acronsh.entity.CarrierInfo;
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
