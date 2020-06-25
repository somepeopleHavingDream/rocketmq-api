package org.yangxin.rocketmq.rocketmqapi.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

/**
 * 生产者
 *
 * @author yangxin
 * 2020/06/16 20:28
 */
@Slf4j
public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("test_quick_producer_name");
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
//        producer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        producer.start();

        for (int i = 0; i < 1; i++) {
            // 1. 创建消息
            // 主题
            Message message = new Message("test_quick_topic",
                    // 标签
                    "TagA",
                    // 用户自定义的key，唯一的标识
                    "key" + i,
                    // 消息内容实体（byte[]）
                    ("Hello RocketMQ" + i).getBytes());

            // 2. 发送消息
            SendResult sendResult = producer.send(message);
            log.info("消息发出： [{}]", sendResult);
        }

        producer.shutdown();
    }
}
