package com.shirc.redisdelayqueuespringdemo.delayqueues;

import com.shirc.redis.delay.queue.iface.impl.AbstractTopicRegister;
import com.shirc.redisdelayqueuespringdemo.bo.MyArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * @Description TODO
 * @Author shirenchuang
 * @Date 2019/7/31 12:22 PM
 **/
@Service
public class DelayQueueCallBackDemo extends AbstractTopicRegister<MyArgs> {
    private static final Logger logger = LoggerFactory.getLogger(DelayQueueCallBackDemo.class);



    @Override
    public String getTopic() {
        return TopicEnums.DEMO_TOPIC.getTopic();
    }

    /**
     * 可以重写这个方法定义 定义这个Topic的核心线程数
     * @return
     */
    @Override
    public int getCorePoolSize() {
        return 5;
    }

    /**
     * 可以重写这个方法定义 定义这个Topic的最大线程数
     * @return
     */
    @Override
    public int getMaxPoolSize() {
        return 10;
    }

    /**
     * 可以重写这个方法定义 方法执行的超时时间
     * @return
     */
    @Override
    public int getMethodTimeout() {
        return super.getMethodTimeout();
    }


    @Override
    public void execute(MyArgs s) {
        long needRunTime = s.getShoudRunTime().getTime();
        long now = System.currentTimeMillis();
        long delayTime = now - needRunTime;
        try {

            Thread.sleep(10000);
        } catch (InterruptedException e) {

        }
        logger.debug("DEMO_TOPIC:成功!,当前时间:{};执行推迟了时间:{},ID:",new Date(),delayTime,s.getId());
    }

    @Override
    public void retryOutTimes(MyArgs myArgs) {
        super.retryOutTimes(myArgs);
        // you can do something ;like send a message to the developer
        logger.error("Oh! no~~~  0.0 重试失败了呀");
    }
}
