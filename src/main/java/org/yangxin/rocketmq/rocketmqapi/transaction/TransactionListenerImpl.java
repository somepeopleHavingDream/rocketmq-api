package org.yangxin.rocketmq.rocketmqapi.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author yangxin
 * 12/28/20 7:49 PM
 */
@Slf4j
public class TransactionListenerImpl implements TransactionListener {

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        log.info("执行本地事务。");
        log.info("callArg: [{}], msg: [{}]", arg, msg);

        // tx.begin
        // 数据库的落库操作

        // tx.commit
        return LocalTransactionState.UNKNOW;
//        return LocalTransactionState.COMMIT_MESSAGE;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        log.info("回调消息检查，msg: [{}]", msg);
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
