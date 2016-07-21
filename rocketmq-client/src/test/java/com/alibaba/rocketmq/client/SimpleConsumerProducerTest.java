/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.rocketmq.client;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;


public class SimpleConsumerProducerTest {
    private static final String TOPIC_TEST = "TopicTest-fundmng";

    @Test
    public void producerConsumerTest() throws MQClientException, InterruptedException {
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("S_fundmng_demo_producer");
        DefaultMQProducer producer = new DefaultMQProducer("P_fundmng_demo_producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC_TEST, null);
        //consumer.setUnitMode(true);
//        //暂停消费
//        consumer.suspend();
//        //继续消费
//        consumer.resume();

        //设置每个队列的最大PULL数量
//        consumer.setPullThresholdForQueue(10000);

        /**
         * 设置每次拉取消息的数据量，默认32
         */
        consumer.setPullBatchSize(1);

        final AtomicLong lastReceivedMills = new AtomicLong(System.currentTimeMillis());

        final AtomicLong consumeTimes = new AtomicLong(0);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Received" + consumeTimes.incrementAndGet() + "messages !");

                lastReceivedMills.set(System.currentTimeMillis());

                /**
                 * 假如部分消息消费成功了怎么办？
                 * 可以通过设置ackIndex来标记从哪儿开始消费成功，从哪儿开始消费失败
                 * ackIndex后面的都是消费失败的，从0开始
                 */
                context.setAckIndex(1);

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


//        producer.start();
//
//        for (int i = 0; i < 100000; i++) {
//            try {
//                Message msg = new Message(TOPIC_TEST, ("Hello RocketMQ " + i).getBytes());
//                SendResult sendResult = producer.send(msg);
//                System.out.println(sendResult);
//            } catch (Exception e) {
//                TimeUnit.SECONDS.sleep(1);
//            }
//        }


        consumer.setPullInterval(5 * 1000L);
        consumer.start();

        Thread.sleep(15 * 1000);

        consumer.suspend();

        Thread.sleep(15 * 1000);
        consumer.resume();

        LockSupport.park();

        consumer.shutdown();
        producer.shutdown();
    }
}
