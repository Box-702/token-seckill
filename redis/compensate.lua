-- 补偿脚本（与 src/token_seckill/resources/compensate.lua 相同）
-- KEYS[1] = 库存 key
-- KEYS[2] = 已领取用户集合 key
-- KEYS[3] = 订单状态 Hash key
-- ARGV[1] = user_id
-- ARGV[2] = failure_reason（失败原因）

-- 从已领取集合中移除用户，并恢复库存
local removed = redis.call('SREM', KEYS[2], ARGV[1])
if removed == 1 then
    redis.call('INCR', KEYS[1])
end

-- 标记订单为失败
redis.call('HSET', KEYS[3], 'status', 'failed', 'failure_reason', ARGV[2])
redis.call('EXPIRE', KEYS[3], 172800)  -- 2天过期
return removed
