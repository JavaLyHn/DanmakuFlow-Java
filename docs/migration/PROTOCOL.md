# 协议与迁移边界

## WebSocket

连接地址：`/ws?uid=1001&name=Alice&room=Live001`。`uid` 和 `room` 必填，`name` 可省略。uid 范围为 0 到 18446744073709551615，保留 Go 的 uint64 数值语义。房间名区分大小写及空格，最多 50 个 Unicode 码点。

发送弹幕：

```json
{"type":101,"data":{"content":"你好 👋"}}
```

接收弹幕：

```json
{"type":101,"room_id":"Live001","data":{"id":0,"room_id":"Live001","user_id":1001,"content":"你好 👋","send_time":"2026-11-06T01:02:03.123456789Z"}}
```

用户、房间、时间和初始 id 由服务器产生；客户端传入的同名字段不能覆盖连接身份。正文最多 500 个 Unicode 码点，空字符串合法。单条 JSON / WebSocket 消息最多 8192 UTF-8 字节；emoji 不能简单按 Java `String.length()` 当作一个字符计算。

点赞：

```json
{"type":103,"data":null}
```

或者 `{"type":103,"data":{"count":10}}`。缺省、null 或零 count 归一为 1；负数、非整数、字符串或超过 Redis 有符号 64 位范围的 count 被拒绝。没有逐次 103 回包，点赞通过房间统计展示。

房间统计：

```json
{"type":102,"room_id":"Live001","data":{"online":2,"likes":10}}
```

online 是连接数，同一 uid 建立两个连接算两人次。目标广播周期为 3 秒，跨节点在线报告 TTL 为 7 秒，属于短时间最终一致的数据。

## Kafka 与数据库

Java 使用独立主题 `danmakuflow.danmaku.v1`，room_id 为消息 key，value 是以下平铺正文：

```json
{"id":0,"room_id":"Live001","user_id":1001,"content":"你好 👋","send_time":"2026-11-06T01:02:03.123456789Z","event_id":"89cb03cc-c45c-48a9-80d6-13ba903420fa"}
```

event_id 在一次事件的 Kafka 重试和消费重放中保持不变，用于数据库去重。数据库生成真正的自增 id；直播广播的 id=0 延续原行为。数据库将 UTC 时间保存到毫秒，Kafka/协议保留纳秒。

存储时间的接收格式要求四位年份、大写 T/Z、最多九位小数秒，数值时区使用 `±HH:mm`；不接受 24:00、带秒的时区或闰秒自动归一。转换后的 UTC 年份必须在 MySQL 的 1000–9999 范围内。

原 Go 的 Kafka value 是 envelope，且没有稳定 event_id，因此不能把旧主题直接切给 Java 消费者。旧数据搬迁、主题切换和数据库切换应另行安排；原 Go 参考代码不可直接对现有数据库启动，因为它会删表。

## 可靠性语义

Java 目标链路是 Kafka 确认成功后再广播 Redis；数据库事务提交后再确认消费 offset。事件重放通过唯一 event_id 去重。Kafka 和 Redis 之间没有原子事务，Redis Pub/Sub 也不补发断线期间的消息，因此不能据此声称端到端 exactly-once。

身份参数仍是原项目的开发协议，尚未引入认证。对外部署需要另行设置可信身份与访问边界。性能对比必须说明 Java 的 Kafka 确认顺序比原 Go 的本地入队即广播更严格。

本文区分协议定义与运行验证；各模块当前完成度和真实基础设施验证范围见 [进展记录](PROGRESS.md)。
