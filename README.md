# zk-messagequeue
基于zookeeper实现的消息队列

## 创建队列
所有的队列都创建在/queue/{queuename}目录下，

## 创建exchange
//TODO...当前仅实现direct功能

## 建立exchange到queue的binding
//TODO...当前仅实现direct功能

## 生产消息
每条消息创建在/queue/{queuename}/{element}{index} 使用zookeeper创建目录时的PERSISTENT_SEQUENTIAL属性，给每条消息设置一个自动递增的下标index，消费的时候从index最小的element开始消费

## 消费消息
读取/queue/{queuename}/下的所有消息，取得最小下标对应的消息（最先push进队列的消息），读取成功后删除对应的element{index}

## 备注
在单进程下没有问题，启动多线程时，删除element有问题

多线程下可以
1.使用轮询的方式保证一条消息只被一个消费者接到(rabbitmq)
2.消息在服务器端分片，不同消费者消费不同片段的消息

