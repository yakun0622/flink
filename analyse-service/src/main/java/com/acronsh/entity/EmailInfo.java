/*
 * Copyright (c) 2017 Hinew. All Rights Reserved.
 * ============================================================================
 * 版权所有 海牛(上海)电子商务有限公司，并保留所有权利。
 * ----------------------------------------------------------------------------
 * ----------------------------------------------------------------------------
 * 官方网站：http://www.hinew.com.cn
 * ============================================================================
 */
package com.acronsh.entity;

import lombok.Data;

/**
 * @author wangyakun
 * @email yakun0622@gmail.com
 * @date 2019/7/22 17:14
 */

@Data
public class EmailInfo {
    /**
     * 邮箱类型
     */
    private String emailType;
    /**
     * 数量
     */
    private Long count;

    private String groupFiled;
}
