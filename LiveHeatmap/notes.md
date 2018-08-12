## Storm 核心概念
1. Streams：数据流、消息流
2. Spout：产生数据 reliable/unreliable  可以直接连接 Twitter API
  1. `nextTuple`
  2. `ack` and `fail`
  3. `emit`
3. Bolt：处理数据（过滤、聚合、查询、数据库操作）
4. Tuple：数据、传递的基本单元
5. Topologies：计算拓扑，将整个流程串起来

### Grouping
1. shuffle
2. fields: 按字段将tuple分发到不同tasks

### Task

## Storm 编程
### ISpout
- 接口 feeding messages into the topology for processing.
-  每个发送出去的message都有一个相应的id，Storm发送ack或者fial时使用id
- nextTuple不能是阻塞的

方法：
- open 初始化
- close 释放资源
- nextTuple 发送数据
- ack
- fail

实现类
BaseRishSpout （抽象类）
DRPCSpout
ShellSpout

### IComponent
接口
为topology提供

方法：
- declareOutputFields 声明当前Spout/Bolt发送tuple的名称
  - 和...配合使用

实现类：
BaseRishSpout

### IBolt
接收tuple处理，并进行相应处理
hold住tuple之后再处理

生命周期
在机器中创建 -> 序列化 -> 提交到nimbus -> 反序列化 -> process

方法：
- prepare
- execute 处理一个tuple数据，tuple中包含metadata
- cleanup

实现类：
BaseRichBolt


## 周边框架
### Logstash
```
logstash -f $LOGSTASH_HOME/project.conf
```

### ZooKeeper
启动zookeeper
```
> zkServer.sh start
> jps
2650 QuorumPeerMain
2669 Jps
```

connect to ZooKeeper client
```
> zkCli.sh -server 127.0.0.1:2181

Welcome to ZooKeeper
[zk: 127.0.0.1:2181(CONNECTED) 0]
```
### Kafka
$KAFKA_HOME/config/server.proporties
```
broker.id
port

log.dirs=/home/yufei/app/tmp/kafka-log
```

启动Kafka服务器
```
> kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

> Jps
2650 QuorumPeerMain
3532 Jps
3470 Kafka
```

创建topic
```
> kafka-topics.sh --create --zookeeper ubuntu:2181 --replication-factor 1 --partitions 1 --topic topic_name
```
发送和接收消息
```
> kafka-console-producer.sh --broker-list ubuntu:9092 --topic project_topic
> kafka-console-consumer.sh --zookeeper ubuntu:2181 --topic project_topic
```

### JDBC


## 需要修改的地方
- config/logstash/project.conf
  - line 3: 路径对应 `get_twitter.py` 中28行的地址
  - line 10: 主机名
- /etc/hosts
  - 添加虚拟机ip和主机名
  - 如果 Kafka 跑在本地不需要修改
- project/src/main/java/streaming/Kafka/StormKafkaTopo.java
  - line 18: `ubunut`改为上一步中添加的主机名
  - line 45, 46: 改为本地 MySQL 服务器的用户名和密码
- web-app/src/main/resources/application-dev.yml
  - line 5, 6
