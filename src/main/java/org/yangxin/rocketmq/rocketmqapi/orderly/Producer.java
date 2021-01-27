package org.yangxin.rocketmq.rocketmqapi.orderly;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author yangxin
 * 1/27/21 3:56 PM
 */
@Slf4j
public class Producer {

    public static void main(String[] args) {
        final String groupName = "test_orderly_producer_name";

        DefaultMQProducer producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = simpleDateFormat.format(date);

        // 这五条消息是一个大的业务操作
        int nums = 5, queueNum = 4;
        for (int i = 0; i < queueNum; i++) {
            sendMessageOrderly(producer, dateStr, nums, i);
        }

        producer.shutdown();
    }

    private static void sendMessageOrderly(DefaultMQProducer producer, String dateStr, int nums, int queueNum) {
        for (int i = 0; i < nums; i++) {
            // 时间戳
            String body = dateStr + " Hello RocketMQ " + i;
            // 参数：topic tag message
            Message message = new Message("test_order_topic",
                    "TagA",
                    "key" + i,
                    body.getBytes(StandardCharsets.UTF_8));

            // 发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector，保证消息进入同一队列
            try {
                SendResult sendResult = producer.send(message, (list, msg, o) -> {
                    int id = (int) o;
                    log.info("id: [{}]", id);

                    return list.get(id);
                    // queueNum是队列下标
                }, queueNum);

                log.info("sendResult: [{}], body: [{}]", sendResult, body);
            } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
