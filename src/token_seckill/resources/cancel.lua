-- 延时取消脚本：释放超时未履约的订单库存
-- KEYS[1] = 订单状态 Hash key
-- KEYS[2] = 库存 key
-- KEYS[3] = 已领取用户集合 key
-- KEYS[4] = 延时取消 ZSET key
-- ARGV[1] = user_id
-- ARGV[2] = order_id
-- ARGV[3] = reason（取消原因）

local status = redis.call('HGET', KEYS[1], 'status')
-- 从延时取消队列中移除（无论结果如何）
redis.call('ZREM', KEYS[4], ARGV[2])

-- 已发放成功：保留库存，不做任何操作
if status == 'success' then
    return 0
end

-- 已取消/已失败：幂等处理，不重复释放库存
if status == 'cancelled' or status == 'failed' then
    return 0
end

-- 仅 processing 状态（或状态缺失）的订单释放库存
local removed = redis.call('SREM', KEYS[3], ARGV[1])
if removed == 1 then
    redis.call('INCR', KEYS[2])
end
redis.call('HSET', KEYS[1], 'status', 'cancelled', 'failure_reason', ARGV[3])
redis.call('EXPIRE', KEYS[1], 172800)  -- 2天过期
return removed
