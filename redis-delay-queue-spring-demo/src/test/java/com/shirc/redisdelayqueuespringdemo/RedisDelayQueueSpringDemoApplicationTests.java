package com.shirc.redisdelayqueuespringdemo;

import com.shirc.redis.delay.queue.common.Args;
import com.shirc.redis.delay.queue.core.RedisDelayQueueContext;
import com.shirc.redis.delay.queue.utils.RedisKeyUtil;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisDelayQueueSpringDemoApplicationTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayQueueSpringDemoApplicationTests.class);


    @Autowired
    RedisTemplate redisTemplate;

    @Autowired
    RedisDelayQueueContext redisDelayQueueContext;






    @Test
    public void testdeleteJobLua(){
        String topic = "DEMO_TOPIC";
        String id = "89";
        List<String> keys = Lists.newArrayList();
        keys.add(RedisKeyUtil.getDelayQueueTableKey());
        keys.add(RedisKeyUtil.getBucketKey());
        DefaultRedisScript redisScript =new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/deleteJob.lua")));
        Object object = redisTemplate.execute(redisScript,keys,RedisKeyUtil.getTopicId(topic, id));
        System.out.println(object);
    }



    @Test
    public void testMoveAndRtTopScoreLua(){
        List<String> keys = Lists.newArrayList();
        keys.add(RedisKeyUtil.getTopicListPreKey());
        keys.add(RedisKeyUtil.getBucketKey());
        DefaultRedisScript<String> redisScript =new DefaultRedisScript<>();
        redisScript.setResultType(String.class);
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/moveAndRtTopScore.lua")));
        String object = (String) redisTemplate.execute(redisScript,redisTemplate.getValueSerializer(),
                redisTemplate.getStringSerializer(),keys,100,System.currentTimeMillis());
        System.out.println(object);
    }


    @Test
    public void testgetJob(){
        List<String> keys = new ArrayList<>(1);
        keys.add(RedisKeyUtil.getDelayQueueTableKey());
        //keys.add(topicId);
        DefaultRedisScript<Args> redisScript =new DefaultRedisScript<>();
        redisScript.setResultType(Args.class);
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/getJob.lua")));
        Object args  =  redisTemplate.execute(redisScript,keys,"DEMO_TOPIC:64ddf635-d0f6-4917-a26a-fa6b936ccfa1"
        );
    }

    private void runMove2ReadyThread(){
        //Move2ReadyThread.getInstance().runMove2ReadyThread();
    }

    private Date getDate(long millis){
        Date date = new Date();
        date.setTime(millis);
        return date;
    }



    public static  <T> T get(Class<T> clz,Object o){
        if(clz.isInstance(o)){
            return clz.cast(o);
        }
        return null;
    }
}
