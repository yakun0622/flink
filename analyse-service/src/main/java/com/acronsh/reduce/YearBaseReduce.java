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

import com.acronsh.entity.YearBase;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/23 00:06
 */
public class YearBaseReduce implements ReduceFunction<YearBase> {
    @Override
    public YearBase reduce(YearBase yearBase, YearBase t1) throws Exception {
        YearBase finalYearBase = new YearBase();
        finalYearBase.setYearType(yearBase.getYearType());
        finalYearBase.setCount(yearBase.getCount() + t1.getCount());
        return finalYearBase;
    }
}
