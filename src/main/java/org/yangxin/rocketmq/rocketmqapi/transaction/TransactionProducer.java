package org.yangxin.rocketmq.rocketmqapi.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.yangxin.rocketmq.rocketmqapi.constants.Const;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

/**
 * @author yangxin
 * 12/23/20 10:42 PM
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        String producerGroupName = "text_tx_producer_group_name";
        TransactionMQProducer producer = new TransactionMQProducer(producerGroupName);
        ExecutorService executorService = new ThreadPoolExecutor(2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName(producerGroupName);
                    return null;
                });
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);
        producer.setExecutorService(executorService);

        // 这个对象主要做两件事情：第一件事情就是异步地执行本地事务；第二件事情就是做回查
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);
        producer.start();

        Message message = new Message("test_tx_topic",
                "TagA",
                "Key",
                ("hello rocketmq for tx!").getBytes(StandardCharsets.UTF_8));
        producer.sendMessageInTransaction(message, "我是回调的参数");

        Thread.sleep(Integer.MAX_VALUE);
        producer.shutdown();
    }
}
