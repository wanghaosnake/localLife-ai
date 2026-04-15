package com.locallife.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("t_failed_order")
public class FailedOrder {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String msgContent; // 消息内容
    private String errorInfo;  // 异常信息
    private LocalDateTime createTime; // 创建时间
    private Integer status;    // 处理状态
}