-- 秒杀 Lua 脚本（旧版参考，完整版见 src/token_seckill/resources/seckill.lua）
-- 此版本不含延时取消队列和 PUBLISH 推送功能
-- KEYS[1] = 库存 key
-- KEYS[2] = 已领取用户集合 key
-- KEYS[3] = 活动元数据 Hash key
-- KEYS[4] = 发放消息 Stream key
-- KEYS[5] = 订单状态 Hash key
-- ARGV[1] = user_id
-- ARGV[2] = order_id
-- ARGV[3] = now_ms（当前时间戳，毫秒）
-- ARGV[4] = activity_id

-- 校验活动状态和时间窗口
local status = redis.call('HGET', KEYS[3], 'status')
local starts_at_ms = tonumber(redis.call('HGET', KEYS[3], 'starts_at_ms') or '0')
local ends_at_ms = tonumber(redis.call('HGET', KEYS[3], 'ends_at_ms') or '0')
local now_ms = tonumber(ARGV[3])

if (status ~= 'ready' and status ~= 'online') or now_ms < starts_at_ms or now_ms > ends_at_ms then
    return 3  -- 活动未开始/已结束/未发布
end

-- 检查库存
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
    return 1  -- 库存不足
end

-- 检查用户是否已领取
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return 2  -- 重复领取
end

-- 扣减库存 + 记录已领取用户 + 写入 Stream + 写入订单 Hash
local sku_id = redis.call('HGET', KEYS[3], 'sku_id')
local token_amount = redis.call('HGET', KEYS[3], 'token_amount')
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
redis.call(
    'XADD', KEYS[4], '*',
    'order_id', ARGV[2],
    'user_id', ARGV[1],
    'activity_id', ARGV[4],
    'sku_id', sku_id,
    'token_amount', token_amount
)
redis.call(
    'HSET', KEYS[5],
    'status', 'processing',
    'user_id', ARGV[1],
    'activity_id', ARGV[4],
    'sku_id', sku_id,
    'token_amount', token_amount
)
redis.call('EXPIRE', KEYS[5], 172800)  -- 2天过期
return 0  -- 秒杀成功
