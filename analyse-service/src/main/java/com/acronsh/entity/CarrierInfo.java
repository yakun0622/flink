package com.acronsh.entity;

import lombok.Data;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 17:14
 */

@Data
public class CarrierInfo {
    /**
     * 运营商类型
     */
    private String carrierType;
    /**
     * 数量
     */
    private Long count;

    private String groupFiled;
}
