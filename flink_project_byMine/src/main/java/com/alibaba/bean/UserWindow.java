package com.alibaba.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/12/25 9:00
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserWindow {
        Timestamp windowStart;
        Timestamp windowEnd;
        String total_size;
        String total_size2;
        int size;
}
