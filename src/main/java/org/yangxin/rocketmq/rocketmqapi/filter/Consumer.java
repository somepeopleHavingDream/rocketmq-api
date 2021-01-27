package org.yangxin.rocketmq.rocketmqapi.filter;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

/**
 * @author yangxin
 * 1/27/21 10:08 PM
 */
@SuppressWarnings("DuplicatedCode")
@Slf4j
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        final String groupName = "test_filter_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);

        // 1. tag
        consumer.subscribe("test_filter_topic", "TagA");

        /*
            2. sql过滤（此过滤方式需要在配置文件种开启相关选项，且此过滤方式会损耗broker的性能，不推荐使用。）
         */
        // 消费者注册监听来进行消费
        consumer.registerMessageListener((MessageListenerConcurrently) (messageExtList, context) -> {
            MessageExt messageExt = messageExtList.get(0);
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
                final int maxReconsumeTimes = 3;

                int reconsumeTimes = messageExt.getReconsumeTimes();
                if (reconsumeTimes >= maxReconsumeTimes) {
                    // 记录日志
                    // 做补偿处理
                    log.error("重新消费次数达至3次，不再处理！");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }
}
