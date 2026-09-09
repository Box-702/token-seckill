# 架构与迁移决策

## 1. 迁移目标

本项目不是把 Java 文件逐行翻译成 Python，而是复现以下可观察行为：

1. Token 包活动可以创建、发布、查询和限量领取。
2. 活动窗口、库存和一人一单在 Redis 内原子判断。
3. HTTP 主链路只返回资格结果与订单 ID，Token 到账由异步消费者完成。
4. 消费失败可留在 pending entries 中恢复，重复投递不能重复发放。
5. MySQL 保留最终订单、库存、账本和余额事实。
6. Agent 能调用业务工具，并且不能绕过用户确认执行抢购。

参考材料中只有设计文档，没有 Java 仓库，因此无法验证其中 Canal/Debezium、Memcache、ScopeCaching 等描述是否真的存在于原代码。本项目只把有代码和测试证据的能力写入简历。

## 2. Java 到 Python 的映射

| Java/Spring 概念 | Python 实现 | 迁移原因 |
| --- | --- | --- |
| Spring Boot Controller | FastAPI Router | 类型化接口、OpenAPI、原生 async |
| MyBatis-Plus | SQLAlchemy 2.0 Async | 显式事务、条件更新、跨 MySQL/SQLite 测试 |
| ThreadLocal ScopeCaching | ContextVar 请求缓存 | asyncio 任务间隔离，避免 ThreadLocal 串请求 |
| Caffeine LoadingCache | cachetools TTLCache | 进程内热点元数据缓存 |
| Memcache | aiomcache，可选 | 复现跨进程 KV 层，但默认可精简 |
| RedisTemplate | redis-py asyncio | 与 FastAPI I/O 并发模型一致 |
| Redisson 用户锁 | redis-py Lock | 消费端同一用户串行发放 |
| Java 阻塞消费者 | 独立 asyncio worker | 与 API 生命周期解耦 |
| RedisIdWorker | 时间戳 + Redis INCR | 多实例趋势递增订单 ID |
| Spring 拦截器 | FastAPI Dependency/Middleware | 身份注入、请求缓存、指标统一处理 |

## 3. 核心写链路

### 3.1 资格判断

`POST /api/v1/activities/{id}/claim` 只执行身份存在性检查和一段 Lua：

- 校验 Redis 活动状态与开始/结束时间；
- 判断库存是否大于零；
- 判断用户是否已经在领取集合中；
- 扣减 Redis 库存；
- 写入已领取用户集合；
- 向 Redis Stream 追加发放消息；
- 写入 `processing` 订单状态缓存；
- 把订单写入延时取消 ZSET（score = 发放截止时间戳）；
- 向实时库存频道 `PUBLISH` 新的剩余库存。

这些命令在同一 Lua 脚本中执行，不存在应用层 `GET -> 判断 -> DECR` 的并发窗口。ZADD 和 PUBLISH 也在这个原子段内完成，因此延时队列与实时推送和库存扣减是一致的。

### 3.2 异步发放

独立 worker 使用 consumer group 读取 Stream：

1. 按 `user_id` 获取分布式锁。
2. 在锁内重读 Redis 订单状态：若已是 `cancelled`/`failed`（被延时取消或永久失败释放），直接移除延时队列并 ACK，不再发放。
3. 先按 `order_id` 和 `(user_id, activity_id)` 查重。
4. 执行 `db_stock = db_stock - 1 WHERE db_stock > 0`。
5. 在同一数据库事务中创建成功订单、写 Token 账本、增加用户余额。
6. 事务完成后更新 Redis 订单状态为 `success`、从延时队列移除并 ACK。
7. 未 ACK 消息由 `XAUTOCLAIM` 接管；永久业务失败执行库存和领取集合补偿后 ACK。

### 3.3 三层幂等

- Redis Lua：入口一人一单。
- 订单表：`(user_id, activity_id)` 唯一约束。
- Token 账本：`order_id` 唯一约束。

即使 ACK 丢失、消息重投或 worker 重启，余额也只能增加一次。

### 3.4 延时取消队列（方向 A：下单 → 支付 → 超时自动释放）

领取成功即创建一条 `processing` 订单，等价于“占用了库存”的未支付订单。配套一个 Redis Sorted Set（key `token:order:delay`，member 为订单 ID，score 为发放截止时间戳）：

- 领取 Lua 原子写入；发放成功或永久失败时移除。
- worker 内另一个 `OrderTimeoutCanceller` 任务按固定间隔用
  `ZRANGEBYSCORE (0, now_ms, batch)` 扫描过期订单。
- 对每个过期订单，重新获取对应的 `lock:grant:user:{user_id}`，在锁内重读
  订单状态：仍为 `processing` 才执行取消；`success` / `failed` / `cancelled`
  只做队列清理，绝不重复释放。
- 取消使用独立 `cancel.lua`，在同一 Lua 内完成“从领取集合移除 + 库存回补 +
  标记订单 `cancelled`”，并对已发放订单幂等返回。

这样形成 `processing -> success | cancelled` 的完整状态机：用户抢到但未发放的
库存会在截止时间后自动释放，避免“抢到的库存被永久扣掉”。

### 3.5 流量防护层（方向 B）

`claim` 入口按三层滑动窗口限流，每层是一个 Redis ZSET，成员为请求标记、分数为
毫秒时间戳，用一次 `ZREMRANGEBYSCORE` + `ZCARD` 完成窗口判断，无乐观锁窗口：

- 用户级：`rl:user:{user_id}`；
- IP 级：`rl:ip:{client_ip}`；
- 活动/接口级：`rl:activity:{activity_id}`。

任一层超限即返回 `429 + Retry-After`，并计入 `agent_token_rate_limit_rejections_total`
指标。可选的行为验证层：`POST /api/v1/rate/challenge` 签发一次性的数学验证码，
`CHALLENGE_ENABLED=true` 时 `claim` 要求携带 `X-Challenge: <id>:<answer>`，
校验后即消费，挡住机器流量。

### 3.6 实时库存推送（方向 C）

领取 Lua 在同一原子段内向 `token:stock:{activity_id}` 频道 `PUBLISH` 新的剩余库存。
`GET /api/v1/activities/{id}/stock/stream` 以 SSE 订阅该频道：

- 连接后先推送 `sync` 帧（活动状态、起止时间、初始库存、当前库存，供前端做前后倒计时）；
- 之后每个库存变化推送 `stock` 帧；
- 使用独立 Redis 连接做 pub/sub，断连时取消订阅并关闭连接。

库存变化由 Lua 在扣减的同时广播，因此 SSE 看到的是真实扣减后的值，而不是轮询快照。

## 4. 读链路

活动详情按以下顺序读取：

`ContextVar -> TTLCache -> Memcached(可选) -> Redis -> MySQL`

实现包含：

- 下层命中后回填上层；
- 不存在活动的空值缓存；
- 共享缓存 TTL 随机抖动；
- 活动发布后显式清理各级缓存。

库存不进入本地缓存或 Memcached 的最终判断，所有实例统一以 Redis Lua 为资格入口，以 MySQL 为最终事实源。

## 5. Agent 设计

LangGraph Agent 暴露四个工具：

- 查询本人 Token 余额；
- 列出可用 Token 包活动；
- 查询异步订单状态；
- 创建待确认的领取动作。

对话历史按 `user_id + thread_id` 存入 Redis。领取工具本身不调用秒杀服务，只生成 10 分钟有效的 `action_id`。用户必须再调用审批接口，审批接口持有动作锁并只允许执行一次。

这个设计把 LLM 的“不确定决策”与库存扣减这种“确定副作用”隔开：模型可以建议和准备动作，但不能自行提交高风险写操作。

## 6. 一致性与对账

`scripts/reconcile.py` 对同一活动比对：

- 初始库存；
- Redis 剩余库存；
- MySQL 剩余库存；
- 成功订单数；
- Token 账本数。

验收关系为：

`初始库存 - 成功订单数 = Redis 库存 = MySQL 库存`，且 `成功订单数 = 账本数`。

## 6. 一致性与对账

`scripts/reconcile.py` 对同一活动比对：

- 初始库存；
- Redis 剩余库存；
- MySQL 剩余库存；
- 成功订单数；
- Token 账本数。

验收关系为：

`初始库存 - 成功订单数 = Redis 库存 = MySQL 库存`，且 `成功订单数 = 账本数`。

延时取消的订单没有生成数据库订单行，也不会写 Token 账本；取消只是把 Redis 库存
回补、释放领取标记并把订单标记为 `cancelled`，因此它不影响“成功订单数 = 账本数”
这一对账关系。对已发放（`success`）的订单，取消逻辑在锁内重读状态后直接跳过，避免
释放已发放库存造成不一致。

## 7. 有意保留的边界

- 参考文档把 Canal/Debezium 描述为亮点，但当前目录没有对应源码；本实现不把它伪装成已完成能力。多实例本地缓存广播和 CDC 可作为后续迭代。
- 当前 Redis Lua 按单实例语义设计。迁移 Redis Cluster 时需要统一 hash tag、按活动拆 Stream，或将事件队列迁至 Kafka；实时推送频道与延时 ZSET 也需要同样的 hash tag 规划。
- SSE 库存推送的“万级连接 < 100ms”属于待压测指标，未实测前不写入任何性能结论；本项目只在实现层面提供 pub/sub 广播与实时帧推送。
- 演示身份使用 `X-User-Id`，正式部署应替换为 OAuth2/JWT，并在网关增加用户、IP、设备多维限流（本项目已提供应用层三级滑动窗口限流，网关级仍需生产化）。
- Agent 已验证图构建、工具边界和审批幂等；远程模型回答质量需要配置真实模型后再做离线评测，不写入当前性能结论。
- 行为验证码为一次性数学题，仅作为“防机器人”的演示层；生产环境应替换为真实人机验证服务，并搭配设备指纹与风控。
