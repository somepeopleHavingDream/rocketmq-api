package org.yangxin.rocketmq.rocketmqapi.model;

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
 * 2020/07/01 20:23
 */
@Slf4j
public class Producer {

    public static void main(String[] args) throws MQClientException {
        // 开启生产者
        String groupName = "test_model_producer_name";
        DefaultMQProducer producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        producer.start();

        // 生产者产生消息
        for (int i = 0; i < 10; i++) {
            String tag = (i % 2 == 0) ? "TagA" : "TagB";
            // topic
                Message message = new Message("test_model_topic2",
                    // tag
                    "tagA",
//                    tag,
                    // body
                    ("信息内容" + i).getBytes());
            try {
                SendResult sendResult = producer.send(message);
                log.info("sendResult: [{}]", sendResult);
            } catch (RemotingException | MQBrokerException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 生产消息结束之后，生产者关闭
        producer.shutdown();
    }
}
