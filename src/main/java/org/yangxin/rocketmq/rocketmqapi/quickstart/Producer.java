package org.yangxin.rocketmq.rocketmqapi.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
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

            // 2.1 同步发送消息
//            message.setDelayTimeLevel(2);

            SendResult result = producer.send(message, (list, msg, o) -> {
                int queueNumber = (int) o;
                return list.get(queueNumber);
            }, 2);
            log.info("result: [{}]", result);

            SendResult sendResult = producer.send(message);
            SendStatus sendStatus = sendResult.getSendStatus();
            log.info("status: [{}]", sendStatus);
            log.info("消息发出： [{}]", sendResult);

//            // 2.2 异步发送消息
//            producer.send(message, new SendCallback() {
//
//                // rabbitmq极速入门的实战：可靠性消息投递
//
//                @Override
//                public void onSuccess(SendResult sendResult) {
//                    log.info("msgId: [{}], status: [{}]", sendResult.getMsgId(), sendResult.getSendStatus());
//                }
//
//                @Override
//                public void onException(Throwable e) {
//                    log.error("发送失败！！", e);
//                }
//            });
        }

//        producer.shutdown();
    }
}
