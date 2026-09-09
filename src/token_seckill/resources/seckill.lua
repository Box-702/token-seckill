-- 秒杀核心 Lua 脚本：原子完成库存校验、扣减、消息写入和通知
-- KEYS[1] = 库存 key
-- KEYS[2] = 已领取用户集合 key
-- KEYS[3] = 活动元数据 Hash key
-- KEYS[4] = 发放消息 Stream key
-- KEYS[5] = 订单状态 Hash key
-- KEYS[6] = 延时取消 ZSET key
-- ARGV[1] = user_id
-- ARGV[2] = order_id
-- ARGV[3] = now_ms（当前时间戳，毫秒）
-- ARGV[4] = activity_id
-- ARGV[5] = expire_at_ms（履约截止时间戳）
-- ARGV[6] = 库存变更通知频道

-- 第1步：校验活动状态和时间窗口
local status = redis.call('HGET', KEYS[3], 'status')
local starts_at_ms = tonumber(redis.call('HGET', KEYS[3], 'starts_at_ms') or '0')
local ends_at_ms = tonumber(redis.call('HGET', KEYS[3], 'ends_at_ms') or '0')
local now_ms = tonumber(ARGV[3])

if (status ~= 'ready' and status ~= 'online') or now_ms < starts_at_ms or now_ms > ends_at_ms then
    return 3  -- 活动未开始/已结束/未发布
end

-- 第2步：检查库存
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
    return 1  -- 库存不足
end

-- 第3步：检查用户是否已领取
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return 2  -- 重复领取
end

-- 第4步：扣减库存 + 记录已领取用户
local sku_id = redis.call('HGET', KEYS[3], 'sku_id')
local token_amount = redis.call('HGET', KEYS[3], 'token_amount')
local remaining = stock - 1

redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])

-- 第5步：写入 Redis Stream（异步落库的消息队列）
redis.call(
    'XADD', KEYS[4], '*',
    'order_id', ARGV[2],
    'user_id', ARGV[1],
    'activity_id', ARGV[4],
    'sku_id', sku_id,
    'token_amount', token_amount
)

-- 第6步：写入订单状态 Hash（订单快照，供查询和状态检查）
redis.call(
    'HSET', KEYS[5],
    'status', 'processing',
    'user_id', ARGV[1],
    'activity_id', ARGV[4],
    'sku_id', sku_id,
    'token_amount', token_amount,
    'expire_at_ms', ARGV[5]
)
redis.call('EXPIRE', KEYS[5], 172800)  -- 2天过期

-- 第7步：加入延时取消队列（超时未履约则自动释放库存）
redis.call('ZADD', KEYS[6], ARGV[5], ARGV[2])

-- 第8步：推送库存变更通知（SSE 实时库存推送）
redis.call('PUBLISH', ARGV[6], remaining)

return 0  -- 秒杀成功
