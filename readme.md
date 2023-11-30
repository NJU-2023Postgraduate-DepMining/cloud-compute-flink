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

![屏幕截图 2023-11-30 204616.png](http://lrjia-bucket-1.oss-cn-hangzhou.aliyuncs.com/obsidian/%E5%B1%8F%E5%B9%95%E6%88%AA%E5%9B%BE%202023-11-30%20204616.png)