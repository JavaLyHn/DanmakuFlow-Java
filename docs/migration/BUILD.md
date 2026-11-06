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

基础构建阶段尚无生产类与测试；空 JAR 的警告属于预期，构建成功不代表基础设施
或协议已经验证。应用入口及可执行 JAR 打包将在实现对应模块时配置。
代理或私服配置应放在本机 Maven 配置中，不提交机器路径、账号或口令。
