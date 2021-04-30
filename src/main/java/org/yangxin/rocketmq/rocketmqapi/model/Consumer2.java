package org.yangxin.rocketmq.rocketmqapi.model;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author yangxin
 * 2020/07/01 20:46
 */
@SuppressWarnings({"AlibabaRemoveCommentedCode", "CommentedOutCode"})
@Slf4j
public class Consumer2 {

    /**
     * 初始化，并启动消费者
     */
    public Consumer2() {
        String groupName = "test_model_consumer_name1";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        try {
            consumer.subscribe("test_model_topic2", "*");
//            consumer.subscribe("test_model_topic2", "TagB");
//            consumer.setMessageModel(MessageModel.BROADCASTING);
            consumer.setMessageModel(MessageModel.CLUSTERING);
            consumer.registerMessageListener(new Listener());
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * @author yangxin
     * 2020/07/01 20:47
     */
    @SuppressWarnings("DuplicatedCode")
    @Slf4j
    static
    class Listener implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messageExtList, ConsumeConcurrentlyContext context) {
            try {
                for (MessageExt msg : messageExtList) {
                    String topic = msg.getTopic();
                    String msgBody = new String(msg.getBody(), StandardCharsets.UTF_8);
                    String tags = msg.getTags();
                    log.info("收到消息：topic: [{}], tags: [{}], msg: [{}]", topic, tags, msgBody);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    @SuppressWarnings("InstantiationOfUtilityClass")
    public static void main(String[] args) {
        new Consumer2();
        log.info("consumer2 start...");
    }
}
