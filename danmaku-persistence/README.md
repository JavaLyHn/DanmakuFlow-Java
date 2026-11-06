# 持久化进程

独立的 Java 21 非 Web 进程：Kafka 批量消费 → MySQL 事务 → 提交消费位点。网关不需要数据库连接。

先在 MySQL 8.4 中准备本项目专用的数据库和用户。应用不会创建数据库；Flyway 只迁移指定库，禁止 clean，不会自动为已有非空库建立 baseline。不要指向其他业务数据库。当前连接 URL 不支持查询参数，驱动连接和读超时由程序管理。

在项目根目录执行 `mvn -B -T1 package` 后，可先查看不连接任何服务的帮助：

```powershell
java -jar danmaku-persistence/target/danmaku-persistence-0.1.0-SNAPSHOT.jar --help
```

运行前配置环境变量（密码通过本机环境或部署平台的密钥配置提供）：

```powershell
$env:DANMAKU_DB_URL = 'jdbc:mysql://127.0.0.1:3306/danmakuflow'
$env:DANMAKU_DB_USER = 'danmakuflow'
$env:DANMAKU_KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
java -Xmx192m -XX:ActiveProcessorCount=2 -jar danmaku-persistence/target/danmaku-persistence-0.1.0-SNAPSHOT.jar
```

| 环境变量 | 默认值 / 要求 |
| --- | --- |
| `DANMAKU_DB_URL` | 必填，`jdbc:mysql://host[:port]/schema` |
| `DANMAKU_DB_USER` | 必填 |
| `DANMAKU_DB_PASSWORD` | 可选，无内置密码 |
| `DANMAKU_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `DANMAKU_KAFKA_TOPIC` | `danmakuflow.danmaku.v1`，须与网关一致 |
| `DANMAKU_KAFKA_GROUP_ID` | `danmakuflow-persistence-v1` |
| `DANMAKU_PERSISTENCE_MAX_RETRIES` | `5`，初次失败后重试次数，范围 0–20 |
| `DANMAKU_PERSISTENCE_RETRY_DELAY_MS` | `200`，指数退避起始毫秒数，范围 1–30000，最大间隔 30 秒 |

每次 poll 最多 500 条，先解码整批，再执行一次数据库事务。事务成功后才提交 Kafka 位点；不确定的提交结果可能重放，通过 `event_id` 唯一约束去重。解码、写入或提交持续失败会在有限重试后使进程退出，消息不会被静默跳过。修复原因后重启；目前没有死信队列或自动跳过坏消息的功能。

连接池最多 4 个连接，Connector/J 开启批量重写；这项配置尚未通过真实 MySQL 吞吐测试验证。退出会唤醒消费者，由消费线程完成事务及客户端关闭，整个退出等待上限 30 秒；超时会报告，JVM 可能在清理未完成时退出。退出中的批次不确认位点，重启后可以重放。

目前验证使用 MockConsumer、H2 和受控启动失败测试，覆盖写入与位点顺序、重放去重、有限重试和退出期限。真实 MySQL/Flyway/Connector-J、Kafka broker 与重平衡、操作系统信号退出仍待集成验证；没有执行压测。
