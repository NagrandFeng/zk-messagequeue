# zk-messagequeue
基于zookeeper实现的消息队列，为了了解message-queue的一些原理，基于zookeeper实现了一个简单的队列demo

## 创建队列
所有的队列都创建在/queues/{queuename}目录下，

## 创建exchange
exchange都创建在 /exchanges/{exchangename}/目录下

## 建立exchange到queue的binding
binding创建在 /bindings/{exchangename}/{routingkeyname}/{queuename} 
由于zk提供的客户端的路径的create方法无法支持一次性完全创建这么深的path，所以代码中采取一条一条往下创建


## 生产消息
每条消息创建在/queues/{queuename}/{element}{index} 使用zookeeper创建目录时的PERSISTENT_SEQUENTIAL属性，给每条消息设置一个自动递增的下标index，消费的时候从index最小的element开始消费

## 消费消息
读取/queues/{queuename}/下的所有消息，取得最小下标对应的消息（最先push进队列的消息），读取成功后删除对应的element{index}

## 备注
在单进程下没有问题，启动多线程时，删除element有问题

多线程下可以
1.使用轮询的方式保证一条消息只被一个消费者接到(rabbitmq)
2.消息在服务器端分片，不同消费者消费不同片段的消息

