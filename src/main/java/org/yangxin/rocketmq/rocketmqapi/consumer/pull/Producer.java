package org.yangxin.rocketmq.rocketmqapi.consumer.pull;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

/**
 * @author yangxin
 * 2020/07/02 16:30
 */
@Slf4j
public class Producer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        String groupName = "test_pull_producer_name";
        DefaultMQProducer producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        producer.start();

        for (int i = 0; i < 10; i++) {
            // topic
            Message message = new Message("test_pull_topic",
                    // tag
                    "TagA",
                    // body
                    ("Hello RocketMQ" + i).getBytes());
            SendResult sendResult;
            try {
                sendResult = producer.send(message);
                log.info("sendResult: [{}]", sendResult);

                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(3000);
            }
        }
    }
}
