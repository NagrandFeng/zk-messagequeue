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

