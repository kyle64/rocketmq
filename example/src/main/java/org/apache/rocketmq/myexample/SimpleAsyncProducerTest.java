package org.apache.rocketmq.myexample;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by ziheng on 2020/8/12.
 */
public class SimpleAsyncProducerTest {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer asyncProducer = new DefaultMQProducer("MyFirstAsyncProducerGroup");
        asyncProducer.setRetryTimesWhenSendAsyncFailed(1);
        asyncProducer.setNamesrvAddr("47.96.131.40:9876");
        asyncProducer.start();

        int messageCount = 3;
        CountDownLatch countDownLatch = new CountDownLatch(messageCount);

        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                Message message = new Message("MyFirstTopic",
                        "*",
                        "*",
                        "Hello world Async".getBytes(RemotingHelper.DEFAULT_CHARSET));
                asyncProducer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                        countDownLatch.countDown();
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.printf("%-10d Exception %s %n", index, e);
                        countDownLatch.countDown();
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

            countDownLatch.await(20, TimeUnit.SECONDS);

        }
    }
}
