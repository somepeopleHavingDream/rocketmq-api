package org.yangxin.rocketmq.rocketmqapi.orderly;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 顺序消息消费
 *
 * @author yangxin
 * 1/27/21 3:56 PM
 */
@SuppressWarnings("InstantiationOfUtilityClass")
@Slf4j
public class Consumer {

    public Consumer() throws MQClientException {
        final String groupName = "test_orderly_consumer_name";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);

        /*
            设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费，
            如果不是第一次启动，那么从上次消费的位置继续消费。
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅的主题，以及过滤的标签内容
        consumer.subscribe("test_order_topic", "TagA");
        // 注册监听
        consumer.registerMessageListener(new Listener());
        consumer.start();

        log.info("Consumer started.");
    }

    private static class Listener implements MessageListenerOrderly {

        private final Random random = new Random();

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
            for (MessageExt messageExt : list) {
                log.info("msg: [{}], content: [{}]", messageExt, new String(messageExt.getBody()));

                try {
                    // 模拟业务逻辑处理
                    TimeUnit.SECONDS.sleep(random.nextInt(4) + 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    public static void main(String[] args) throws MQClientException {
        new Consumer();
    }
}
