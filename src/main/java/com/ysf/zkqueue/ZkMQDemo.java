package com.ysf.zkqueue;

import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author Yeshufeng
 * @title
 * @date 2018/2/23
 */
public class ZkMQDemo {

    // 会话超时时间，设置为与系统默认时间一致
    private static final int SESSION_TIMEOUT = 30 * 1000;

    // 创建 ZooKeeper 实例
    private ZooKeeper zk;

    /**
     * ZK的根目录
     */
    private final String ROOT = "/";

    /**
     * 队列下每条数据存储的目录名
     */
    private final String ELEMENT = "/element";

    public ZkMQDemo() {
        init();
    }

    public void createQueue(String queue) {

        String queuePath = ROOT + queue;

        try {
            Stat stat = zk.exists(queuePath, false);
            if (stat == null) { //当前发送方式direct，不允许队列同名
                zk.create(queuePath, queue.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void push(String queue, String data) {
        try {
            zk.create(ROOT + queue + ELEMENT, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 模拟FIFO消费
     * @param queue
     */
    public void consume(String queue) {
        Stat stat = null;
        while (true) {
            try {
                List<String> childrenList = zk.getChildren(ROOT + queue, wh);
                if (childrenList.isEmpty()) {
                    System.out.println("currentThread:" + Thread.currentThread().getName() + " deal ,list is empty,wait");
                } else {
                    //substring(7)目的是去掉element，剩下的都转Integer，将数字前的0都过滤掉，然后开始比较大小，从最小的开始消费
                    //比较过后最后由appendHead方法把数字前的0都补充完全
                    Integer min = new Integer(childrenList.get(0).substring(7));

                    //这种比较方式待优化
                    for (String path : childrenList) {
                        Integer tempValue = new Integer(path.substring(7));
                        if (tempValue < min) {
                            min = tempValue;
                        }
                    }
                    String minStr = appendHead(min);
                    System.out.println("Temporary value: " + ROOT + queue + ELEMENT + minStr);
                    byte[] b = zk.getData(ROOT + queue + ELEMENT + minStr, false, stat);
                    String result = new String(b);
                    System.out.println("currentThread:" + Thread.currentThread().getName() + " deal ,queue : [" + ROOT + queue + ELEMENT + minStr + "] value is :" + result);

                    zk.delete(ROOT + queue + ELEMENT + minStr, 0);

                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    /**
     * 保证每个消费者都从序号最小的element开始消费，需要在下标前补0
     * @param min
     * @return
     */
    private String appendHead(Integer min) {
        String str = String.format("%010d", min);
        System.out.println("after append:" + str); // 0001
        return str;
    }

    // 创建 Watcher 实例
    private Watcher wh = new Watcher() {
        /**
         * Watched事件
         */
        public void process(WatchedEvent event) {
            System.out.println("WatchedEvent：" + event.toString());
        }
    };

    // 初始化 ZooKeeper 实例
    private void init() {
        // 连接到ZK服务，多个可以用逗号分割写
        try {
            zk = new ZooKeeper("127.0.0.1:2181", ZkMQDemo.SESSION_TIMEOUT, this.wh);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    void close() throws InterruptedException {
        zk.close();
    }


}
