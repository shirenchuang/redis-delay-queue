package com.shirc.redisdelayqueuespringdemo.delayqueues;

import com.shirc.redis.delay.queue.iface.impl.AbstractTopicRegister;
import com.shirc.redisdelayqueuespringdemo.bo.MyArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * @Description TODO
 * @Author shirenchuang
 * @Date 2019/7/31 12:22 PM
 **/
@Service
public class DelayQueueCallBackDemo2 extends AbstractTopicRegister<MyArgs> {
    private static final Logger logger = LoggerFactory.getLogger(DelayQueueCallBackDemo2.class);

    @Override
    public String getTopic() {
        return TopicEnums.DEMO_TOPIC_2.getTopic();
    }

    @Override
    public int getCorePoolSize() {
        return 100;
    }

    @Override
    public void execute(MyArgs s) {
        long needRunTime = s.getShoudRunTime().getTime();
        long now = System.currentTimeMillis();
        long delayTime = now - needRunTime;

        logger.debug("DEMO_TOPIC_2:成功!,当前时间:{}\n;执行推迟了时间:{},ID:",new Date(),delayTime,s.getId());
    }
}
