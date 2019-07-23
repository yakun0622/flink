package com.acronsh.entity;

import lombok.Data;

import java.util.List;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 17:14
 */

@Data
public class BaiJiaInfo {
    /**
     * 败家指数  0-20  20-50  50-70  70-80  80-90  90-100
     */
    private String baijiaScore;
    private String userId;

    private String createTime;
    private String amount;
    private String payType;
    private String payTime;
    private String payStatus;
    private String couponAmount;
    private String totalAmount;
    private String refundAmount;
    private String num;
    /**
     * 数量
     */
    private Long count;

    private String groupFiled;

    private List<BaiJiaInfo> list;
}
