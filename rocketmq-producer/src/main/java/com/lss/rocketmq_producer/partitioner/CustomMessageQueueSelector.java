package com.lss.rocketmq_producer.partitioner;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CustomMessageQueueSelector implements MessageQueueSelector {

    private static final Logger log = LoggerFactory.getLogger(CustomMessageQueueSelector.class);

    @Override
    public MessageQueue select(final List<MessageQueue> mqs,
                               final Message msg,
                               final Object arg) {

        if (mqs == null || mqs.isEmpty()) {
            throw new IllegalArgumentException("队列列表为空");
        }

        String keys = msg.getKeys();
        String argStr = arg != null ? arg.toString() : "";

        if (keys != null && keys.startsWith("priority-")) {
            log.debug("High priority message, keys={}", keys);
            return mqs.get(0);
        }

        if (keys != null && keys.startsWith("vip-")) {
            log.debug("VIP user message, keys={}", keys);
            return mqs.get(1);
        }

        int queueNum = mqs.size();
        int hashCode = argStr.hashCode();
        int selectedIndex = Math.abs(hashCode % queueNum);

        log.debug("Select queue: arg={}, hashCode={}, selectedIndex={}",
                argStr, hashCode, selectedIndex);

        return mqs.get(selectedIndex);
    }
}
