package com.locallife.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.locallife.dto.LoginFormDTO;
import com.locallife.dto.Result;
import com.locallife.entity.User;

import javax.servlet.http.HttpSession;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IUserService extends IService<User> {

    Result sendCode(String phone, HttpSession session);

    Result login(LoginFormDTO loginForm, HttpSession session);

    Result me();
}
