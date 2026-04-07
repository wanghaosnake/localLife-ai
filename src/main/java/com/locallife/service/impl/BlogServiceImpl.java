package com.locallife.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.locallife.dto.Result;
import com.locallife.dto.UserDTO;
import com.locallife.entity.Blog;
import com.locallife.entity.Follow;
import com.locallife.entity.User;
import com.locallife.mapper.BlogMapper;
import com.locallife.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.locallife.service.IFollowService;
import com.locallife.service.IUserService;
import com.locallife.utils.SystemConstants;
import com.locallife.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.locallife.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = super.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            queryBlogUser(blog);
            isBlogLiked(blog);
        });

        return Result.ok(records);
    }



    @Override
    public Result queryBlogById(Long id) {
        //1.查询blog
        Blog blog = getById(id);
        if(ObjectUtil.isEmpty(blog)){
            return Result.fail("笔记不存在");
        }

        queryBlogUser(blog);
        //查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }



    @Override
    public Result likeBlog(Long id) {
        //1.获取登录的用户
        Long userId = UserHolder.getUser().getId();

        //2.判断当前用户是否点赞
        String key= BLOG_LIKED_KEY+id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());


        if(score==null) {
            //3.如果没有点赞,可以点赞
            //3.1数据库点赞数+1
            //3.2保存用户id到redis的set集合用于判断是否点赞
            boolean isSuccess = this.update().setSql("liked=liked+1").eq("id", id).update();
            if(isSuccess){
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else {
            //4.如果已经点赞,取消点赞
            //4.1数据库点赞数-1
            //4.2把用户从redis的set集合中移除
            boolean isSuccess = this.update().setSql("liked=liked-1").eq("id", id).update();
            if(isSuccess){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key= BLOG_LIKED_KEY+id;
        //1.查询top5的点赞用户 zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);

        //2.解析出其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        if(ObjectUtil.isEmpty(ids)){
            return Result.ok(Collections.emptyList());
        }
        String idsStr = StrUtil.join(",", ids);
        //3.根据用户id查询用户
        List<UserDTO> users = userService.query().in("id",ids).last("ORDER BY FIELD(id,"+idsStr+")").list()
                .stream()
                .map(user-> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        //4.返回
        return Result.ok(users);
    }

    @Override
    public Result saveBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();

        blog.setUserId(user.getId());

        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增笔记失败!");
        }

        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();

        for(Follow follow:follows){
            Long userId = follow.getUserId();
            String key="feed:"+userId;

            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }

        return Result.ok(blog.getId());
    }

    //查询blog,封装blog所属的用户属性
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if(ObjectUtil.isEmpty(user)){
            return;
        }
        Long nowUserId = UserHolder.getUser().getId();
        String key= BLOG_LIKED_KEY+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, nowUserId.toString());
        blog.setIsLike(score!=null);

    }
}
