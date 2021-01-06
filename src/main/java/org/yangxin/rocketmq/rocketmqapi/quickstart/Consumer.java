package org.yangxin.rocketmq.rocketmqapi.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

/**
 * @author yangxin
 * 2020/06/16 20:29
 */
@Slf4j
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_quick_consumer_name");
//        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("test_quick_topic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            MessageExt messageExt = msgs.get(0);
            try {
                String topic = messageExt.getTopic();
                String tags = messageExt.getTags();
                String keys = messageExt.getKeys();
                int queueId = messageExt.getQueueId();
                String msgBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                log.info("topic: [{}], tags: [{}], keys: [{}], queueId: [{}], msgBody: [{}]",
                        topic, tags, keys, queueId, msgBody);
            } catch (Exception e) {
                e.printStackTrace();

                int reconsumeTimes = messageExt.getReconsumeTimes();
                if (reconsumeTimes >= 3) {
                    // 记录日志
                    // 做补偿处理
                    log.error("重新消费次数达至3次，不再处理！");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        log.info("consumer start...");
    }
}
