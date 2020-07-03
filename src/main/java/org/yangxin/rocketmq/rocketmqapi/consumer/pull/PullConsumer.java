package org.yangxin.rocketmq.rocketmqapi.consumer.pull;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangxin
 * 2020/07/02 16:36
 */
@Slf4j
public class PullConsumer {

    /**
     * Map<key, value> key为指定的队列，value为这个队列下一个拉取数据的位置
     */
    private static final Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<>();

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws MQClientException {
        String groupName = "test_pull_consumer_name";
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(groupName);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
//        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        consumer.start();
        log.info("consumer start...");

        // 从TopicTest这个主题中，去获取所有的队列（默认会有四个队列）
        Collection<MessageQueue> messageQueueCollection = consumer.fetchSubscribeMessageQueues("test_pull_topic");
        // 遍历每一个队列，进行拉取数据
        for (MessageQueue messageQueue : messageQueueCollection) {
            log.info("Consume from the queue: [{}]", messageQueue);

            SINGLE_MQ:
            while (true) {
                // 从queue中获取数据，从什么位置开始拉取数据，单次最多拉取32条记录
                try {
                    PullResult pullResult = consumer.pullBlockIfNotFound(messageQueue,
                            null,
                            getMessageQueueOffset(messageQueue),
                            32);
                    log.info("pullResult: [{}]", pullResult);
                    log.info("pullStatus: [{}]", pullResult.getPullStatus());

                    putMessageQueueOffset(messageQueue, pullResult.getNextBeginOffset());

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> messageExtList = pullResult.getMsgFoundList();
                            for (MessageExt messageExt : messageExtList) {
                                log.info("body: [{}]", new String(messageExt.getBody()));
                            }
                            break;
                        case NO_NEW_MSG:
                            log.info("没有新的数据了……");
                            break SINGLE_MQ;
                        default:
                            break ;
                    }
                } catch (RemotingException | MQBrokerException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        consumer.shutdown();
    }

    private static void putMessageQueueOffset(MessageQueue messageQueue, long nextBeginOffset) {
        OFFSET_TABLE.put(messageQueue, nextBeginOffset);
    }

    private static long getMessageQueueOffset(MessageQueue messageQueue) {
        Long offset = OFFSET_TABLE.get(messageQueue);
        return offset != null ? offset : 0;
    }
}
