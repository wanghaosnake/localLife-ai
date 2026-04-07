package com.locallife.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.locallife.dto.Result;
import com.locallife.dto.UserDTO;
import com.locallife.entity.Follow;
import com.locallife.mapper.FollowMapper;
import com.locallife.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.locallife.service.IUserService;
import com.locallife.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key="follows:"+userId;

        if (isFollow) {
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(userId);
            boolean isSuccess = save(follow);
            if (isSuccess) {

                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            remove(new QueryWrapper<Follow>().eq("user_id", userId)
                    .eq("follow_user_id", followUserId));

            stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
        }

        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        Integer count = query().eq("follow_user_id", followUserId).eq("user_id", UserHolder.getUser().getId()).count();

        return Result.ok(count>0);
    }

    @Override
    public Result followCommons(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key="follows:"+userId;
        String key2="follows:"+id;

        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if(ObjectUtil.isEmpty(intersect)){
            return Result.ok(Collections.emptyList());
        }

        List<Long> ids = intersect.stream()
                .map(str -> Long.valueOf(str))
                .collect(Collectors.toList());
        List<UserDTO> users = userService.listByIds(ids).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}
