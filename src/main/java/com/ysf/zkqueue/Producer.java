package com.ysf.zkqueue;

import java.util.Random;

/**
 * @author Yeshufeng
 * @title
 * @date 2018/2/23
 */
public class Producer {

    public static void main(String[] args) throws Exception {

        ZkMQDemo producer = new ZkMQDemo();
        producer.createQueue("ysf-queue");
        Random random = new Random();
        for(int i = 0; i < 20; i++) {
            Integer randomInt = random.nextInt(100);
            String mydata = String.valueOf(randomInt);
            System.out.println("push data:["+mydata+"] to queue");
            producer.push("ysf-queue",mydata);
            Thread.sleep(1000);
        }
        producer.close();
    }

}
