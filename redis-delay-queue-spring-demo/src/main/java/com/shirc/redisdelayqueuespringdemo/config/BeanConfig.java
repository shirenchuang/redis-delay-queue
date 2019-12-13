package com.shirc.redisdelayqueuespringdemo.config;

import com.shirc.redis.delay.queue.core.RedisDelayQueueContext;
import com.shirc.redis.delay.queue.iface.RedisDelayQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

/**
 * @Description 接入Redis_Delay_Queue
 * @Author shirenchuang
 * @Date 2019/8/1 9:41 AM
 **/
@Component
public class BeanConfig {



    private RedisTemplate redisTemplate;


    /**修改 redisTemplate 的key序列化方式  **/
    @Autowired(required = false)
    public void setRedisTemplate(RedisTemplate redisTemplate) {
        RedisSerializer stringSerializer = new StringRedisSerializer();
        GenericJackson2JsonRedisSerializer jackson2JsonRedisSerializer = new GenericJackson2JsonRedisSerializer();
        redisTemplate.setKeySerializer(stringSerializer);
        redisTemplate.setHashKeySerializer(stringSerializer);
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        this.redisTemplate = redisTemplate;
    }


    /******* 接入 RedisDelayQueue  *******/

    @Bean
    public RedisDelayQueueContext getRdctx(){
        RedisDelayQueueContext context =  new RedisDelayQueueContext(redisTemplate,"dq_demo");
        return context;
    }

    @Bean
    public RedisDelayQueue getRedisOperation(RedisDelayQueueContext context){
        return context.getRedisDelayQueue();
    }



}
