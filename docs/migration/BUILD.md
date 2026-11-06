# Java 构建

需要 JDK 21 和 Maven 3.9+，在本机设置 `JAVA_HOME`，将 Maven 加入 `PATH`。
在仓库根目录运行：

```sh
mvn -B -T1 package
```

`.mvn/jvm.config` 限制 Maven 堆为 256 MiB、可用处理器为 2；测试单 JVM、
单 fork，堆为 192 MiB、可用处理器为 2，关闭 JUnit 并行执行。请使用 `-T1`，
避免模块测试并发。构建无需启动 Docker、Redis、Kafka 或 MySQL。

父 POM 统一使用 Spring Boot 4.1.2、Netty BOM 4.2.20.Final 和 Jackson 3
（`tools.jackson`）；Lettuce、Kafka、JDBC、MySQL、Flyway、JUnit 版本由 Boot 管理。
`danmaku-protocol` 不依赖 Spring，`danmaku-gateway` 使用 Netty 与 Spring 非 Web
上下文，`danmaku-persistence` 包含 JDBC 和 Kafka，`danmaku-tools` 使用 JDK WebSocket。

检查 Netty 依赖版本：

```sh
mvn -B -T1 dependency:tree -Dincludes=io.netty:*
```

构建会执行协议、网关、持久化和客户端的有限功能测试。Netty 测试会短暂绑定
本机随机回环端口；数据服务使用受控替身或内存数据库。构建成功不代表真实
Redis/Kafka/MySQL 链路、部署退出流程或性能已经验证。

只运行某个模块及它依赖的模块，可用 `mvn -B -T1 -pl danmaku-gateway -am test`。
首次构建需要下载依赖；依赖已缓存时可加 `-o` 离线执行。构建产物在各模块的
`target/` 中，运行命令见模块说明。不要同时启动多个 Maven 构建。

代理或私服配置应放在本机 Maven 配置中，不提交机器路径、账号或口令。
