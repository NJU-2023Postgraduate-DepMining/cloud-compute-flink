# NpmFlink

从 kafka 中读取 github 依赖信息，存入 redis

## 根据 proto 文件生成 java 文件

protobuf version: 3.25.1

```shell
protoc --java_out=src/main/java .\github_msg.proto
```

## 打包

```shell
mvn clean package
```

输出 `target/GitHubJob-0.1.jar`

## 运行参数

```shell
usage: utility-name
 -kafka <arg>           kafka address, default: kafka:9092
 -redis_address <arg>   redis address, default: redis
 -redisPort <arg>       redis port, default: 6379
 -topic <arg>           kafka topic, default: topic_github
```

## 输出 example

![屏幕截图 2023-11-30 204616](https://github.com/lrjia/cloud-compute-flink/assets/52886379/9f3536af-55c2-4d43-a2e2-76c6b86b740d)
