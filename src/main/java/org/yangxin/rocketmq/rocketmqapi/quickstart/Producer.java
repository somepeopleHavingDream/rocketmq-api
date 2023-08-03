package org.yangxin.rocketmq.rocketmqapi.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;


/**
 * @author yangxin
 * 2020/06/16 20:28
 */
@SuppressWarnings({"AlibabaRemoveCommentedCode", "CommentedOutCode", "AlibabaUndefineMagicConstant"})
@Slf4j
public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("test_quick_producer_name");
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        producer.start();

        for (int i = 0; i < 5; i++) {
            // 1. 创建消息
            // 主题（如果RocketMq一开始没有这个Topic，则默认创建该Topic）
            Message message = new Message("test_quick_topic",
                    // 标签
                    "TagA",
                    // 用户自定义的key，唯一的标识
                    "key" + i,
                    // 消息内容实体（byte[]）
                    ("Hello RocketMQ" + i).getBytes());

            // 延迟消息（发送到broker后，延迟一段时间再对consumer可见）
            // 设置延迟时间级别（1s、 5s、 10s、 30s、 1m、 2m、 3m、 4m、 5m、 6m、 7m、 8m、 9m、 10m、 20m、 30m、 1h、 2h）
//            if (i == 2) {
//                message.setDelayTimeLevel(2);
//            }

            // 消息自定义投递规则，发送到某个指定队列（下面例子是将消息指定发送到队列2中）
//            SendResult result = producer.send(message, (list, msg, o) -> {
//                        int queueNumber = (int) o;
//                        return list.get(queueNumber);
//                    },
//                    // arg是回调给MessageQueueSelector.select方法的参数
//                    2);
//            log.info("result: [{}]", result);
//

            /*
                同步发送会阻塞发送线程，直到消息发送成功或发送失败。这意味着同步发送需要等待代理的响应，因此它通常比异步发送更慢。但是，同步发送可以保证消息已经成功发送到代理。
                异步发送不会阻塞发送线程。它会立即返回一个Future对象，该对象可以用于检查消息是否已经成功发送到代理。由于异步发送不需要等待代理的响应，因此它通常比同步发送更快。但是，异步发送不能保证消息已经成功发送到代理。
             */

            // 2.1 同步发送消息
            SendResult sendResult = producer.send(message);
            SendStatus sendStatus = sendResult.getSendStatus();
            log.info("status: [{}]", sendStatus);
            log.info("消息发出： [{}]", sendResult);

            // 2.2 异步发送消息
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

        producer.shutdown();
    }
}
