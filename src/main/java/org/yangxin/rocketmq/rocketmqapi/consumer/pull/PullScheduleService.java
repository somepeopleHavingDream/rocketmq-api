package org.yangxin.rocketmq.rocketmqapi.consumer.pull;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.util.List;

/**
 * @author yangxin
 * 2020/07/02 16:54
 */
@Slf4j
public class PullScheduleService {

    public static void main(String[] args) throws MQClientException {
        String groupName = "test_pull_consumer_name";

        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(groupName);
        scheduleService.getDefaultMQPullConsumer().setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        scheduleService.registerPullTaskCallback("test_pull_topic", (mq, context) -> {
            MQPullConsumer consumer = context.getPullConsumer();
            log.info("queueId: [{}]", mq.getQueueId());

            try {
                // 获取从哪里拉取
                long offset = consumer.fetchConsumeOffset(mq, false);
                if (offset < 0) {
                    offset = 0;
                }

                PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                if (pullResult.getPullStatus() == PullStatus.FOUND) {
                    List<MessageExt> messageExtList = pullResult.getMsgFoundList();
                    for (MessageExt messageExt : messageExtList) {
                        // 消费数据
                        log.info("body: [{}]", new String(messageExt.getBody()));
                    }
                }

                consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                // 设置再过3000ms后重新拉取
                context.setPullNextDelayTimeMillis(3000);
            } catch (MQClientException | InterruptedException | RemotingException | MQBrokerException e) {
                e.printStackTrace();
            }
        });

        scheduleService.start();
    }
}
