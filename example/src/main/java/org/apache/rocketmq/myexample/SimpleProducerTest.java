package org.apache.rocketmq.myexample;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Created by ziheng on 2020/8/2.
 */
public class SimpleProducerTest {
    public static void main(String[] args) throws MQClientException {
        // 发送消息流程
        // 先实例化producer，设置producer group，name server等参数
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("47.96.131.40:9876");
        // 启动producer
        // 1. 默认CREATE_JUST状态，会先更新成START_FAILED失败状态，启动完成后再更新为RUNNNING
        // 2. producer内部创建一个单例的MQClientFactory实例，负责producer各个操作的主要对象。
        // 3. 把producer实例注册到producer group对应的map中
        // 4. 将默认的topic信息存到本地topicPublishInfoTable的map中取，默认一个topic会有4个queue
        // 5. 调用mQClientFactory真正的启动方法（mQClientFactory还持有一个内部的producer对象）
        // 6. mQClientFactory会检查name server信息，没有则向一个固定地址请求name server信息
        // 7. 启动一系列服务：响应通道（核心是netty），定时任务，拉取消息服务，负载均衡服务等
        // 定时任务包括：检查name server变化，检查topic的路由变化，清理下线的broker，持久化消费进度，调整线程池
        // 8. 更新producer状态为RUNNING
        producer.start();

        for (int i = 0; i < 3; i++) {
            try {
                Message msg = new Message("MyFirstTopic",
                        "*",
                        "*",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 发送消息
                // 1. 获取topic的发布信息topicPublishInfo
                // 2. 在topic对应的broker中，选择一个需要发送消息的mq队列
                // 3. 通过netty的方式发送消息给broker
                // 发送消息有三种模式：同步，异步，oneWay
                // 都是通过broker address建立channel
                // 同步的发送用cdl阻塞等待返回sendResult，
                // 异步的发送需要先获取semaphore的许可，
                // 异步的回调则由producer启动时，启动的netty的响应通道时，注册的NettyClientHandler处理
                // 发送失败会自动尝试，默认是1+2=3次
                // 如果开启了延迟容错机制，那么对于broker的延时长度，会有一个对应的不可用时间，在这个不可用时间段内，该broker暂时unavailable
                // 在这个时间内还有发送消息的请求，那么会随机出一个broker发送(i = threadLocalIndex % half)
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.shutdown();

    }
}
