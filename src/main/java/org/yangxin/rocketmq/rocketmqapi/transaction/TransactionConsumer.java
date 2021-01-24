package org.yangxin.rocketmq.rocketmqapi.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author yangxin
 * 12/23/20 10:43 PM
 */
@Slf4j
public class TransactionConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_tx_consumer_group_name");
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(20);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("test_tx_topic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messageExtList, context) -> {
            MessageExt messageExt = messageExtList.get(0);
            try {
                String topic = messageExt.getTopic();
                String tags = messageExt.getTags();
                String keys = messageExt.getKeys();
                String body = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                log.info("收到事务消息，topic: [{}], tags: [{}], keys: [{}], body: [{}]", topic, tags, keys, body);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        log.info("tx consumer started.");
    }
}
