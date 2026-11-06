# Java 交互客户端

这是单连接的手动 WebSocket 客户端，不产生压测流量。需要 Java 21。

```sh
mvn -B -T1 -pl danmaku-tools -am package
java -Xmx64m -XX:ActiveProcessorCount=2 -jar danmaku-tools/target/danmaku-tools-0.1.0-SNAPSHOT.jar --help
java -Xmx64m -XX:ActiveProcessorCount=2 -jar danmaku-tools/target/danmaku-tools-0.1.0-SNAPSHOT.jar --url "ws://127.0.0.1:8080/ws?uid=1&name=java-cli&room=test"
```

输入普通文本发送弹幕，`/like` 点赞一次，`/like 10` 点赞十次，`/quit` 退出。显式点赞数量必须为正整数。服务端 JSON 会按完整消息输出；客户端不替服务端校验消息字段。

不带参数时连接上述回环地址；`--help` 不联网。正常关闭码 1000/1001 及本地退出返回 0，连接/读写失败和 1008/1011/1013 等异常关闭返回非零并显示状态码，不打印服务端提供的关闭原因。单条命令错误可提示后继续。客户端只有一个连接，不自动重连，连接和写入都有有限超时。

已验证命令/分片/退出行为以及可执行 JAR 的帮助入口；单客户端连接临时 Netty 网关的本机冒烟检查也已通过，覆盖弹幕回显、uint64 身份、点赞统计接收和 `/quit` 退出。该检查使用数据服务替身，没有验证真实 Redis/Kafka/MySQL。完整字段与语义见 [协议说明](../docs/migration/PROTOCOL.md)。
