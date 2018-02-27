package com.ysf.zkqueue;

/**
 * @author Yeshufeng
 * @title
 * @date 2018/2/23
 */
public class Consumer implements Runnable{

    private ZkMQDemo zkMQDemo;

    public Consumer() {
        zkMQDemo = new ZkMQDemo();
    }

    public void run() {
        zkMQDemo.consume("ysf-queue");
    }

    public static void main(String[] args) throws Exception{
        Consumer consumer = new Consumer();
        Thread t1 = new Thread(consumer);
        t1.start();

        Consumer consumer2 = new Consumer();
        Thread t2 = new Thread(consumer2);
        t2.start();

    }

}
