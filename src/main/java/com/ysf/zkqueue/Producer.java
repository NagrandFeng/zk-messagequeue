package com.ysf.zkqueue;

import java.util.Random;

/**
 * @author Yeshufeng
 * @title
 * @date 2018/2/23
 */
public class Producer {

    private static  String exchange = "test-exchange";

    private static String queue = "test-queue";

    private static String routingKey = "r1";

    public static void main(String[] args) throws Exception {

        ZkMQDemo producer = new ZkMQDemo();
        producer.createExchange(exchange);
        producer.createQueue(queue);
        producer.createBinding(exchange,queue,routingKey);

        Random random = new Random();
        for(int i = 0; i < 20; i++) {
            Integer randomInt = random.nextInt(100);
            String mydata = String.valueOf("message-"+randomInt);
            System.out.println("push data:["+mydata+"] to queue");
            producer.push(exchange,routingKey,mydata);
            Thread.sleep(1000);
        }
        producer.close();
    }

}
