--1.参数列表
--1.优惠券id
local voucherId=ARGV[1]

--1.2用户iD
local userId=ARGV[2]

--1.3订单id
local orderId=ARGV[3]

--2.数据key
--2.1库存key
local stockKey= 'seckill:stock:'..voucherId

--2.2订单key
local orderKey='seckill:order:'..voucherId

--3.脚本业务
--3.1 判断库存是否充足
if(tonumber(redis.call('get',stockKey))<=0) then
    --3.2 库存不足,返回1
    return 1
end
--3.3判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember',orderKey,userId)==1) then
    --3.4如果此优惠券的value中存在此用户id,说明该用户已经购买过了这个优惠券了
    return 2
end
--3.5扣库存 incrby stockKey -1
redis.call('incrby',stockKey,-1)

--3.6下单(保存用户)sadd orderKey userId
redis.call('sadd',orderKey,userId)
--
----3.7下单以后,发送生成的信息到消息队列中
--redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId);
return 0

