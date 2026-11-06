# WebSocket 网关

Java 21、Spring Boot 非 Web 上下文、Netty、Lettuce 和 Kafka。网关不依赖 MySQL，也不执行数据库迁移。

实际运行前需要 standalone Redis 和 Kafka，并准备主题 `danmakuflow.danmaku.v1`。这里不自动启动这些服务。先构建，再查看不联网的帮助：

```sh
mvn -B -T1 -pl danmaku-gateway -am package
java -jar danmaku-gateway/target/danmaku-gateway-0.1.0-SNAPSHOT.jar --help
```

服务准备好后，可在 PowerShell 中启动：

```powershell
$env:DANMAKU_REDIS_URI = 'redis://127.0.0.1:6379/0'
$env:DANMAKU_KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
java -Xmx256m -XX:ActiveProcessorCount=2 -jar danmaku-gateway/target/danmaku-gateway-0.1.0-SNAPSHOT.jar
```

默认地址 `ws://127.0.0.1:8080/ws?uid=1&name=demo&room=test`。可用 [Java 交互客户端](../danmaku-tools/README.md) 手动发消息。上面的 JVM 参数适合有限功能检查，不是容量结论；Netty 直接内存、线程栈等也会占用进程内存。

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `DANMAKU_GATEWAY_HOST` | `127.0.0.1` | 监听地址 |
| `DANMAKU_GATEWAY_PORT` | `8080` | 端口，0–65535 |
| `DANMAKU_GATEWAY_MAX_ROOMS` | `10000` | 本节点房间状态上限 |
| `DANMAKU_GATEWAY_MAX_CONNECTIONS` | `100000` | 连接上限，包含未完成握手的 TCP 连接 |
| `DANMAKU_GATEWAY_MAX_PENDING` | `4096` | 核心在途业务/广播限制 |
| `DANMAKU_GATEWAY_CLIENT_MAX_PENDING_FRAMES` | `64` | 每连接、每个发送准入阶段的待写帧上限 |
| `DANMAKU_GATEWAY_CLIENT_MAX_PENDING_BYTES` | `524288` | 每连接、每个发送准入阶段的待写载荷字节上限 |
| `DANMAKU_REDIS_URI` | `redis://127.0.0.1:6379/0` | 支持 redis/rediss；含凭据时由本机环境或密钥配置提供 |
| `DANMAKU_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka 地址列表 |
| `DANMAKU_KAFKA_TOPIC` | `danmakuflow.danmaku.v1` | 必须与持久化进程一致 |
| `DANMAKU_KAFKA_MAX_IN_FLIGHT` | `256` | 排队、发送和等待 broker 确认的总记录上限 |
| `DANMAKU_KAFKA_QUEUE_CAPACITY` | `128` | 专用发送工作队列大小 |
| `DANMAKU_KAFKA_CLOSE_TIMEOUT_MS` | `5000` | 发送器退出预算，1–60000 毫秒 |

除端口和退出预算外，上表的数值限额要求为正。过大的配置仍可能耗尽主机资源，默认最大连接数没有经过容量压测。

## 行为和边界

- 每个房间使用一次 Redis 订阅；收到订阅确认后才处理新连接的消息。Kafka 确认弹幕后才发布 Redis，避免本地重复广播。
- 发送有两个限额阶段：应用消息进入事件循环之前，以及所有 WebSocket 帧进入编码器之前。后者也包含 Netty 自动回复的 Pong，超限立即关闭连接。两个阶段各用上表的帧/载荷预算，已进入管线的应用帧会重叠计数；应用排队和控制帧合计可能接近两个预算之和，另有帧头等开销。
- 在线数每 3 秒刷新，各实例报告 TTL 为 7 秒；online 统计连接数。点赞使用 Redis 的有符号 64 位整数，溢出会失败。统计聚合避免 Lua 浮点数舍入。
- Redis 请求队列最多 1024，命令和连接超时 2 秒，共享 2 个 I/O 与 2 个计算线程。采用两个连接分别处理普通命令和 Pub/Sub。
- Redis 重连不补发丢失的直播消息；客户端重放过滤器排除 `INCRBY` 和 `PUBLISH`，避免对不确定结果自动重复点赞或广播。操作仍可能已执行但调用方得到超时，因此不构成 exactly-once。退订/报告清理失败的房间会保留受限的失败状态并拒绝重新加入，需要在恢复服务后重启网关清理。
- Redis Lua 跨多个键执行，当前仅支持 standalone Redis，不支持 Redis Cluster。
- Kafka 使用 `acks=all`、幂等发送、8 MiB 缓冲、最多 1 秒元数据/缓冲等待和 15 秒交付超时。broker 副本配置仍由部署方管理，应用配置不等于已完成可靠性验证。
- 退出先关闭 Netty，再有限等待核心清理，然后关闭 Redis 和 Kafka；部分启动失败也会尝试释放已取得的资源。核心等待上限 3 秒，Redis 关闭上限 6 秒，Kafka 使用配置预算。

Kafka 和 Redis 不构成原子事务。消息可能已进入存储链路却未完成直播广播，客户端重发也可能产生新事件。协议与限制见 [协议说明](../docs/migration/PROTOCOL.md)，实际测试范围见 [验证记录](../docs/migration/VALIDATION.md)。
