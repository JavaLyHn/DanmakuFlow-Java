# DanmakuFlow

从 [charlesAcmen/live-stream-danmaku](https://github.com/charlesAcmen/live-stream-danmaku) 逐步迁移的 Java 直播弹幕后端。目标是兼容原有 WebSocket 协议，保留 Redis 跨节点广播、房间统计、Kafka 异步持久化能力，并修复明显的数据可靠性问题。

当前处于迁移阶段，尚未完成 Java 全链路验证。性能是设计重点，但不承诺未经测量的连接数或吞吐量。

- [迁移范围与架构](docs/superpowers/specs/2026-11-06-java-migration.md)
- [分步实现计划](docs/superpowers/plans/2026-11-06-java-migration.md)
- [进展记录与已暂停的 Go 基线](docs/migration/PROGRESS.md)

Java 新实现采用本仓库 MIT 许可。原项目代码保留原作者的 MIT 许可及出处。
