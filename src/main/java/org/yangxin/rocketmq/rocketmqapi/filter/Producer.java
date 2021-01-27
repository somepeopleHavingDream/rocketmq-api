package org.yangxin.rocketmq.rocketmqapi.filter;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.nio.charset.StandardCharsets;

/**
 * @author yangxin
 * 1/27/21 10:03 PM
 */
@SuppressWarnings("AlibabaRemoveCommentedCode")
@Slf4j
public class Producer {

    public static void main(String[] args) throws MQClientException {
        final String groupName = "test_filter_producer";
        DefaultMQProducer producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        producer.start();

        int nums = 5;
        for (int i = 0; i < nums; i++) {
            String tag = i % 2 == 0 ? "TagA" : "TagB";
            // topic
            Message msg = new Message("test_filter_topic1",
                    // tag
                    tag,
                    // key
                    "OrderID001",
                    // body
                    ("Hello RocketMQ" + i).getBytes(StandardCharsets.UTF_8));
            // putUserProperty
//            msg.putUserProperty("a", String.valueOf(i));

            try {
                SendResult result = producer.send(msg);
                log.info("result: [{}]", result);
            } catch (RemotingException | MQBrokerException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.shutdown();
    }
}
