package org.yangxin.rocketmq.rocketmqapi.consumer.pull;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * @author yangxin
 * 1/18/21 9:47 PM
 */
@SuppressWarnings({"InfiniteLoopStatement", "AlibabaRemoveCommentedCode"})
@Slf4j
public class LitePullConsumer {

    /**
     * Map<key, value> key为指定的队列，value为这个队列下一个拉取数据的位置
     */
//    private static final Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<>();

    public static void main(String[] args) throws MQClientException {
        String groupName = "test_lite_pull_consumer_name";
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(groupName);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        consumer.start();
        log.info("consumer start...");

        Collection<MessageQueue> messageQueueCollection = consumer.fetchMessageQueues("test_pull_topic");
        List<MessageQueue> messageQueueList = new ArrayList<>(messageQueueCollection);
        consumer.assign(messageQueueList);
        consumer.seek(messageQueueList.get(0), 0);

        try {
            while (true) {
                List<MessageExt> messageExtList = consumer.poll();
                for (MessageExt messageExt : messageExtList) {
                    String topic = messageExt.getTopic();
                    String tags = messageExt.getTags();
                    String keys = messageExt.getKeys();
                    String msgBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);

                    log.info("topic: [{}], tags: [{}], keys: [{}], msgBody: [{}]", topic, tags, keys, msgBody);
                }

                consumer.commitSync();
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } finally {
            consumer.shutdown();
        }
    }
}
