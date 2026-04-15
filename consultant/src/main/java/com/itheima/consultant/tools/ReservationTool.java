package com.itheima.consultant.tools;

import com.itheima.consultant.pojo.Reservation;
import com.itheima.consultant.service.ReservationService;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
public class ReservationTool {

    @Autowired
    private ReservationService reservationService;

    // 格式化器：支持 AI 传入的中文时间 → 转成标准时间
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy年M月d日HH:mm");

    //1.工具方法: 添加预约信息（AI 友好版）
    @Tool("预约到店消费服务")
    public void addReservation(
            @P("用户姓名") String name,
            @P("用户手机号") String phone,
            @P("预约时间，格式：2026年5月7日19:30") String communicationTime,
            @P("预约的商家名称") String shopName
    ) {
        // 把中文时间 → 转成数据库能存的标准时间
        LocalDateTime time = LocalDateTime.parse(communicationTime, FORMATTER);

        Reservation reservation = new Reservation(null, name, phone, time, shopName);
        reservationService.insert(reservation);
    }

    //2.工具方法: 查询预约信息
    @Tool("根据用户手机号查询预约单")
    public List<Reservation> findReservation(@P("用户手机号") String phone) {
        return reservationService.findByPhone(phone);
    }

}