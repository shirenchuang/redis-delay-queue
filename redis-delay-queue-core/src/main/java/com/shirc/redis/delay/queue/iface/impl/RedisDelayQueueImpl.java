package com.shirc.redis.delay.queue.iface.impl;

import com.shirc.redis.delay.queue.common.Args;
import com.shirc.redis.delay.queue.common.DelayQueueException;
import com.shirc.redis.delay.queue.common.RunTypeEnum;
import com.shirc.redis.delay.queue.core.RedisDelayQueueContext;
import com.shirc.redis.delay.queue.iface.RedisDelayQueue;
import com.shirc.redis.delay.queue.redis.RedisOperation;
import com.shirc.redis.delay.queue.utils.NextTimeHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * @Description 提供给客户端使用的 延迟队列操作
 * @Author shirenchuang
 * @Date 2019/7/30 5:33 PM
 **/
public  class RedisDelayQueueImpl implements RedisDelayQueue {
    private static final Logger logger = LoggerFactory.getLogger(RedisDelayQueueContext.class);



    private RedisOperation redisOperation;
    private ConcurrentHashMap<String, AbstractTopicRegister> topicRegisterHolder;
    private ExecutorService executor;


    public RedisDelayQueueImpl(RedisOperation redisOperation, ConcurrentHashMap<String, AbstractTopicRegister> topicRegisterHolder, ExecutorService executor) {
        this.redisOperation = redisOperation;
        this.topicRegisterHolder = topicRegisterHolder;
        this.executor = executor;
    }

    @Override
    public void addAsync(Args args, String topic, long delayTimeMillis){
        add(args,delayTimeMillis,topic,RunTypeEnum.ASYNC);
    }


    @Override
    public void add(Args args,String topic,long runTimeMillis,RunTypeEnum runTypeEnum) {
        if(runTypeEnum == RunTypeEnum.ASYNC){
            executor.execute(()->addJob(args, topic, runTimeMillis));
        }else {
            addJob(args, topic, runTimeMillis);
        }
    }

    @Override
    public void add(Args args, long delayTimeMillis, String topic, RunTypeEnum runTypeEnum){
        if(runTypeEnum == RunTypeEnum.ASYNC){
            executor.execute(()->addJob(args, delayTimeMillis, topic));
        }else {
            addJob(args, delayTimeMillis, topic);
        }
    }

    private void addJob(Args args, long delayTimeMillis, String topic) {
        preCheck(args,topic,null,delayTimeMillis);
        long runTimeMillis = System.currentTimeMillis()+delayTimeMillis;
        redisOperation.addJob(topic,args,runTimeMillis);
        //尝试更新下次的执行时间
        NextTimeHolder.tryUpdate(runTimeMillis);
    }
    private void addJob(Args args, String topic, long runTimeMillis) {
        preCheck(args,topic,runTimeMillis,null);
        redisOperation.addJob(topic,args,runTimeMillis);
        //尝试更新下次的执行时间
        NextTimeHolder.tryUpdate(runTimeMillis);
    }
    private void preCheck(Args args,String topic,Long runTimeMillis,Long delayTimeMillis) {
        if(checkStringEmpty(topic)||
                checkStringEmpty(args.getId())){
            throw new DelayQueueException("未设置Topic或者Id!");
        }
        if(runTimeMillis==null){
            if(delayTimeMillis==null){
                throw new DelayQueueException("未设置延迟执行时间!");
            }
        }
        if(topic.contains(":")){
            throw new DelayQueueException("Topic 不能包含特殊字符 :  !");
        }
        //check topic exist
        if(!checkTopicExist(topic)){
            throw new DelayQueueException("Topic未注册!");
        }
    }


    @Override
    public void delete(String topic, String id,RunTypeEnum runTypeEnum) {
        if(runTypeEnum == RunTypeEnum.ASYNC){
            executor.execute(()-> redisOperation.deleteJob(topic, id));
        }else {
            redisOperation.deleteJob(topic, id);
        }
        logger.info("删除延时任务:Topic:{},id：{}",topic,id);
    }

    @Override
    public void deleteAsync(String topic, String id) {
        delete(topic,id,RunTypeEnum.ASYNC);
    }

    private boolean checkStringEmpty(String string){
        return string==null||string.length()==0;
    }



    public  boolean checkTopicExist(String topic){
        for(Map.Entry<String, AbstractTopicRegister> entry: topicRegisterHolder.entrySet()) {
            if(entry.getKey().equals(topic)){
                return true;
            }
        }
        return false;
    }
}
