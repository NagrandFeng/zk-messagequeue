# zk-messagequeue
基于zookeeper实现的消息队列，为了了解message-queue的一些原理，基于zookeeper实现了一个简单的队列demo

## 创建队列
所有的队列都创建在/queues/{queuename}目录下，

## 创建exchange
exchange都创建在 /exchanges/{exchangename}/目录下

## 建立exchange到queue的binding
binding创建在 /bindings/{exchangename}/{routingkeyname}/{queuename}

binding创建时遇到些问题，当/binding不存在时，无法创建出下一级目录，如/binding/{exchangename}

## 生产消息
每条消息创建在/queues/{queuename}/element{index}下

element节点为zookeeper中类型为PERSISTENT_SEQUENTIAL的znode，给每条消息设置一个自动递增的下标index，消费的时候从index最小的element开始消费
这种类型的znode自动为element后生成一个十位数的下标，如第一个znode就叫element0000000001



## 消费消息
读取/queues/{queuename}/下的所有消息，取得最小下标对应的消息（最先push进队列的消息），读取成功后删除对应的element{index}

## 备注

### 多消费者并发问题
在单进程下没有问题，启动多线程时，删除element有问题

多线程下可以

1.使用轮询的方式保证一条消息只被一个消费者接到(rabbitmq)

2.消息在服务器端分片，不同消费者消费不同片段的消息

### 消息发送和消费的确认问题
暂时无法实现消息确认，主要是对消息确认的实现原理还没有掌握透彻