package com.ysf.zkqueue;

import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * exchange名
     */
    private final String EXCHANGES = "/exchanges";

    /**
     * 队列
     */
    private final String QUEUES = "/queues";

    /**
     * bindings绑定
     */
    private final String BINDINGS = "/bindings";

    public ZkMQDemo() {
        init();
    }


    public void createExchange(String exchange) {
        String exchangePath = EXCHANGES+"/"+exchange;
        createPath(exchangePath, CreateMode.PERSISTENT);

    }

    public void createBinding(String exchange, String queue, String routingKey) {
        //验证exchange是否存在
        //验证queue是否存在
        if (isPathExist( EXCHANGES + "/" + exchange) && isPathExist( QUEUES + "/" + queue)) {
            //创建binding
            String path =  BINDINGS + "/" + exchange + "/" + routingKey + "/" + queue;
            if (!isPathExist(path)) {
                //zk的客户端无法一次性将path创建
                createPath(BINDINGS+"/"+exchange,CreateMode.PERSISTENT);
                createPath(BINDINGS+"/"+exchange+"/" + routingKey,CreateMode.PERSISTENT);
                createPath(path, CreateMode.PERSISTENT);
            }
        }
    }


    public void createQueue(String queue) {
        //创建队列
        if (!isPathExist( QUEUES + "/" + queue)) {
            createPath( QUEUES + "/" + queue, CreateMode.PERSISTENT);
        }
    }


    /**
     * direct 模式下的生产消息
     *
     * @param queue
     * @param data
     */
    public void push(String queue, String data) {
        createPath(ROOT + queue + ELEMENT, CreateMode.PERSISTENT);
    }

    /**
     * topic 模式下的生产消息
     * @param exchange
     * @param routingKey
     * @param data
     */
    public void push(String exchange, String routingKey, String data) {
        //根据exchange和routingkey查询queue
        if (isPathExist( BINDINGS + "/" + exchange + "/" + routingKey)) {
            //getchild
            List<String> queueList = getChildPath(BINDINGS + "/" + exchange + "/" + routingKey);
            String queue;
            if (queueList.size() > 0) {
                queue = queueList.get(0);
                //在queue下创建消息
                if (isPathExist( QUEUES + "/" + queue)) {
                    createPath( QUEUES + "/" + queue + ELEMENT,data,CreateMode.PERSISTENT_SEQUENTIAL);
                }
            } else {
                System.out.println("绑定关系下未找到队列");
            }

        } else {
            System.out.println("绑定关系不存在，发送失败");
        }

    }

    /**
     * 模拟FIFO消费
     *
     * @param queue
     */
    public void consume(String queue) {
        Stat stat = null;
        while (true) {
            try {
                List<String> childrenList = zk.getChildren(QUEUES+"/"+queue, wh);
                if (childrenList.isEmpty()) {
                    System.out.println("currentThread:" + Thread.currentThread().getName() + " deal ,list is empty,wait");
                    Thread.sleep(5000);
                } else {
                    //substring(7)目的是去掉element，剩下的都转Integer，将数字前的0都过滤掉，然后开始比较大小，从最小的开始消费
                    //比较过后最后由appendHead方法把数字前的0都补充完全
                    Integer min = new Integer(childrenList.get(0).substring(7));

                    //这种比较方式待优化,
                    for (String path : childrenList) {
                        Integer tempValue = new Integer(path.substring(7));
                        if (tempValue < min) {
                            min = tempValue;
                        }
                    }
                    String minStr = appendHead(min);
                    System.out.println("Temporary value: " + ROOT + queue + ELEMENT + minStr);
                    byte[] b = zk.getData(QUEUES+"/" + queue + ELEMENT + minStr, false, stat);
                    String result = new String(b);
                    System.out.println("currentThread:" + Thread.currentThread().getName() + " deal ,queue : [" + ROOT + queue + ELEMENT + minStr + "] value is :" + result);

                    zk.delete(QUEUES+"/" + queue + ELEMENT + minStr, 0);

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
     *
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
            createPath(QUEUES,CreateMode.PERSISTENT);
            createPath(EXCHANGES,CreateMode.PERSISTENT);
            createPath(BINDINGS,CreateMode.PERSISTENT);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    List<String> getChildPath(String parentPath) {
        try {
            return zk.getChildren(parentPath, false);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new ArrayList<String>();
    }

    boolean isPathExist(String path) {
        boolean result = false;

        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {

                result = false;
            } else if (stat.getCtime() != 0) {
                result = true;
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;

    }

    void createPath(String path, CreateMode createMode) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) { //当前发送方式direct，不允许队列同名
                zk.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void createPath(String path,String data,CreateMode createMode) {
        try {
            zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    void close() throws InterruptedException {
        zk.close();
    }


}
