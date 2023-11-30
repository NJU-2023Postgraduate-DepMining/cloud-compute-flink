# NpmFlink

## 根据 proto 文件生成 java 文件

protobuf version: 3.25.1

```shell
protoc --java_out=src/main/java .\github_msg.proto
```

## 打包

```shell
mvn clean package
```

## 运行参数

```shell
usage: utility-name
 -kafka <arg>           kafka address, default: kafka:9092
 -redis_address <arg>   redis address, default: redis
 -redisPort <arg>       redis port, default: 6379
 -topic <arg>           kafka topic, default: topic_github
```
