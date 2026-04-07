package com.locallife.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.locallife.dto.LoginFormDTO;
import com.locallife.dto.Result;
import com.locallife.dto.UserDTO;
import com.locallife.entity.User;
import com.locallife.mapper.UserMapper;
import com.locallife.service.IUserService;
import com.locallife.utils.RegexUtils;
import com.locallife.utils.SystemConstants;
import com.locallife.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.locallife.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {


    @Resource
    StringRedisTemplate stringRedisTemplate;



    public UserServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    @Override
    public Result sendCode(String phone, HttpSession session) {
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误!");
        }
        String code = RandomUtil.randomNumbers(6);

        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY +phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);

        log.debug(code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        if(RegexUtils.isPhoneInvalid(loginForm.getPhone())){
            return Result.fail("手机号格式错误！");
        }

        String redisCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + loginForm.getPhone());
        String code = loginForm.getCode();
        if(ObjectUtil.isEmpty(redisCode)|| !redisCode.equals(code)){
            return Result.fail("验证码出现错误!");
        }

        LambdaQueryWrapper<User> wrappers = Wrappers.<User>lambdaQuery()
                .eq(User::getPhone, loginForm.getPhone());

        User user = super.getOne(wrappers);

        if(ObjectUtil.isEmpty(user)){
            user=createUser(loginForm.getPhone());

        }
        String token = UUID.randomUUID().toString();

        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(), CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,userMap);
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,LOGIN_USER_TTL,TimeUnit.MINUTES);
        return Result.ok(token);
    }

    @Override
    public Result me() {
        UserDTO user = UserHolder.getUser();
        return Result.ok(user);
    }

    private User createUser(String phone) {
        User newUser = new User().setPhone(phone);
        newUser.setNickName(SystemConstants.USER_NICK_NAME_PREFIX +RandomUtil.randomString(10));
        super.save(newUser);
        return newUser;
    }
}
